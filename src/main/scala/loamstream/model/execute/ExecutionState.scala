package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.ValueBox
import ExecutionState.JobStateList
import ExecutionState.JobState
import loamstream.util.TimeUtils
import loamstream.util.Sequence

/**
 * @author clint
 * Jan 24, 2020
 * 
 * The state of the execution of a LoamStream pipeline.  This is intended to be the single point of truth when
 * executing.  See ExecutionState.{JobStateList, JobState} for an explanation of the data structures used.
 */
final class ExecutionState private (
    val maxRunsPerJob: Int,
    private[this] val jobStateListBox: ValueBox[JobStateList],
    index: Map[LJob, Int]) extends Loggable {
  
  def size: Int = jobStateListBox.get(_.size)
  
  private[execute] def allJobs: Iterable[LJob] = index.keys
  
  /**
   * Are all jobs "done"?  (Ie, finished or deliberately never started) 
   */
  def isFinished: Boolean = jobStateListBox.get { _ =>  
    val justCells: Iterator[ExecutionCell] = jobStateListBox.get(_.iterator.map(_.cell))
    
    justCells.forall(cell => cell.isFinished || cell.couldNotStart)
  }
  
  private[execute] def statusOf(job: LJob): JobStatus = jobStateListBox.get(_.cellFor(index)(job)).status
  
  /**
   * Returns a view of the current state of all jobs, useful to Executers. (The result of jobStatuses())
   * 
   * Before that, this method obtains that view, uses it to determine which jobs are runnable, and marks them
   * as running.  Jobs that will never be runnable are marked CouldNotStart.  RxExecuter invokes this method 
   * repeatedly to get new lists of jobs to run.
   */
  def updateJobs(): ExecutionState.JobStatuses = jobStateListBox.get { _ =>
    TimeUtils.time(s"updateJobs()", debug(_)) {
      val currentJobStatuses = jobStatuses
      
      val eligible = currentJobStatuses.readyToRun
      
      TimeUtils.time(s"startRunning()", debug(_)) {
        startRunning(eligible.keys)
      }
      
      val toCancel = currentJobStatuses.cannotRun.keys
      
      TimeUtils.time(s"markAs(CouldNotStart)", debug(_)) { 
        markAs(toCancel, JobStatus.CouldNotStart)
      }
      
      currentJobStatuses
    }
  }

  /**
   * Returns a view of the current state of all jobs, useful to Executers.  Notably, jobs that are now
   * runnable, and those that are now known to NEVER be runnable (due to a failure of one of their dependencies,
   * say) are returned.
   */
  def jobStatuses: ExecutionState.JobStatuses = { 
    val jobStates = jobStateListBox.get(_.snapshot)      
  
    TimeUtils.time("Computing JobStatuses", debug(_)) {
      val numRunning = jobStates.count(_.cell.isRunning)
      val numFinished = jobStates.count(_.cell.isFinished)
      
      val z = ExecutionState.JobStatuses.empty.copy(numRunning = numRunning, numFinished = numFinished)
      
      jobStates.foldLeft(z) { (acc, jobState) =>
        import jobState.{job, cell}
        
        def anyDepsStopExecution: Boolean = {
          def depCells = cellsFor(jobStates)(job.dependencies.map(_.job)) 
          
          cell.notStarted && depCells.exists(_.canStopExecution)
        }
        
        def canRun: Boolean = this.canRun(jobStates)(jobState)
        
        if(canRun) { acc.withRunnable(jobState) }
        else if(anyDepsStopExecution) { acc.withCannotRun(jobState) }
        else { acc }
      }
    }
  }
  
  /**
   * Obtains the ExecutionCells for a given bunch of jobs, given the states of all jobs.
   * (Returns an array for fast iteration (profiling turned this up). 
   */
  private def cellsFor(jobStates: JobStateList)(jobs: Set[JobNode]): Array[ExecutionCell] = {
    val indexes = jobs.iterator.map(_.job).map(index(_))
    
    val cells: Array[ExecutionCell] = Array.ofDim[ExecutionCell](jobs.size)
    
    indexes.map(i => jobStates(i).cell).copyToArray(cells)
    
    cells
  }
  
  /**
   * Can a job run, given its current state, and the states of all other jobs.
   */
  private[execute] def canRun(jobStates: JobStateList)(jobState: JobState): Boolean = {
    import jobState.{job, cell}
    
    cell.notStarted && {
      val deps = job.dependencies
      
      deps.isEmpty || {
        val depCells = cellsFor(jobStates)(deps)
        
        depCells.forall(_.isTerminal) && !depCells.exists(_.canStopExecution)
      }
    }
  }
  
  /**
   * Mark a job as running - change its status to Running and increment its run count.
   */
  private[execute] def startRunning(jobs: TraversableOnce[LJob]): Unit = transition(jobs, _.markAsRunning)

  /**
   * Mark jobs as having the given status, which must be either CouldNotStart, or otherwise a non-finished status.    
   */
  private def markAs(jobs: TraversableOnce[LJob], jobStatus: JobStatus): Unit = transition(jobs, _.markAs(jobStatus))
  
  private def transition(jobs: TraversableOnce[LJob], doTransition: ExecutionCell => ExecutionCell): Unit = {
    if(jobs.nonEmpty) {
      val jobSet = jobs.toSet
      
      def doDoTransition(jobState: JobState): JobState = jobState.transformCell(doTransition)
      
      jobStateListBox.foreach { jobStates =>
        val jobIndices: Iterator[Int] = jobSet.iterator.map(index(_))
        
        jobIndices.foreach { jobIndex => 
          val tuple = jobStates(jobIndex)
          
          jobStates(jobIndex) = doDoTransition(tuple)
        }
      }
    }
  }
  
  /**
   * Mark jobs finished, using an Execution obtained from a ChunkRunner.  
   */
  private[execute] def finish(results: Iterable[(LJob, Execution)]): Unit = {
    finish(results.iterator.map { case (job, execution) => (job -> execution.status) }) 
  }
  
  //For convenient testing
  private[execute] def finish(job: LJob, status: JobStatus): Unit = finish(Seq(job -> status))
  
  /**
   * Mark jobs finished.  Mark each job with the associated status.  (These statuses are assumed to be terminal.)
   * If the status is a failure and the job has run too many times (>= maxRunsPerJob) mark the job FailedPermanently.
   * If the status is a failure and the job has NOT run too many times, mark it as runnable.
   * Otherwise, mark the job as having its associated status. 
   */
  def finish(results: TraversableOnce[(LJob, JobStatus)])(implicit discriminator: Int = 1): Unit = {
    for {
      jobStates <- jobStateListBox
      (job, status) <- results
    } {
      val jobIndex = index(job)
        
      val cell = jobStates(jobIndex).cell
      
      lazy val runCount: Int = cell.runCount
      
      lazy val tooManyRuns: Boolean = runCount >= maxRunsPerJob 
      
      lazy val isTerminalFailure: Boolean = status.isFailure && tooManyRuns
      
      val transition: ExecutionCell => ExecutionCell = {
        if(isTerminalFailure) { 
          debug(s"Restarting $job ? NO (job has run $runCount times, max is $maxRunsPerJob)")
          
          _.finishWith(JobStatus.FailedPermanently) 
        } else if (status.isFailure) {
          debug(s"Restarting $job ? YES (job has run $runCount times, max is $maxRunsPerJob)")
          
          _.markAsRunnable 
        }
        else {_.finishWith(status) }
      }
      
      jobStates(jobIndex) = JobState(job, transition(cell))
      
      if(isTerminalFailure) {
        cancelSuccessors(jobStates)(job)
      }
    }
  }
  
  /**
   * Cancel all the jobs that depend on this job, and the ones that depend on them, etc, by
   * marking them as CouldNotStart.
   */
  private[execute] def cancelSuccessors(fields: JobStateList)(failedJob: LJob): Unit = {
    TimeUtils.time(s"Cancelling successors for failed job with id ${failedJob.id}", debug(_)) {
      val successors = ExecuterHelpers.flattenTree(Set(failedJob), _.successors).toSet - failedJob
      
      markAs(successors.iterator.map(_.job), JobStatus.CouldNotStart)
    }
  }
}

object ExecutionState {
  /**
   * The data structure that tracks the state of all the jobs being executed by LoamStream on a given run.
   * The data is represented as a sequence of JobStates (Job, ExecutionCell) tuples.  An array is used for a
   * few reasons, mostly related to performance.  Initally, I tried a Map[LJob, ExecutionCell], but while this
   * allowed fast lookups of the state of a job, iterating through the Map to find runnable jobs (something LS
   * does very frequently) was too slow. Updates to lots of jobs in the immutable Map (the norm when running 
   * large pipelines) also produced a large amount of GC pressure.  
   *  
   * A mutable array addresses both of these: iterating over an array is as fast as it gets on the JVM, and only
   * making new ExecutionCells on job state changes instead of new Maps substantially reduced the amount of 
   * allocations and GC activity.  Together, these factors increased performance a lot.
   * 
   * Looking up JobStates by jobs, and finding the subset of JobStates for a given bunch of jobs is made fast 
   * (enough) by pairing a JobStateList with an index, a Map[LJob, Int], mapping jobs to their positions in a
   * JobStateList.   
   */
  private final class JobStateList(byJob: Array[JobState]) extends Iterable[JobState] {
    def snapshot: JobStateList = new JobStateList(copyOf(byJob))
    
    override def size: Int = byJob.length
    
    override def iterator: Iterator[JobState] = byJob.iterator
    
    def apply(i: Int): JobState = byJob(i)
    
    def update(i: Int, jobState: JobState): Unit = byJob.update(i, jobState)
    
    def cellFor(index: Map[LJob, Int])(job: LJob): ExecutionCell = byJob(index(job)).cell
  }
  
  private def copyOf(a: Array[JobState]): Array[JobState] = {
    val result: Array[JobState] = Array.ofDim(a.length)
    
    a.copyToArray(result)
    
    result
  }
  
  /**
   * Basically a tuple of a job and its current status, as represented by an ExecutionCell
   */
  final case class JobState(job: LJob, cell: ExecutionCell) {
    def toTuple: (LJob, ExecutionCell) = (job, cell)
    
    def transformCell(f: ExecutionCell => ExecutionCell): JobState = copy(cell = f(cell))
  }
  
  object JobState {
    def initialFor(job: LJob): JobState = JobState(job, ExecutionCell.initial)
  }
  
  def initialFor(executable: Executable, maxRunsPerJob: Int): ExecutionState = {
    val cellsByJob: Array[JobState] = executable.allJobs.iterator.map(JobState.initialFor).toArray
    
    val indicesByJob: Map[LJob, Int] = {
      Map.empty ++ cellsByJob.iterator.zipWithIndex.map { case (jobState, i) => (jobState.job -> i) } 
    }
    
    new ExecutionState(maxRunsPerJob, ValueBox(new JobStateList(cellsByJob)), indicesByJob)
  }
  
  final case class JobStatuses(
      readyToRun: Map[LJob, ExecutionCell], 
      cannotRun: Map[LJob, ExecutionCell],
      numRunning: Int,
      numFinished: Int) {
    
    def withRunnable(jobAndCell: JobState): JobStatuses = copy(readyToRun = readyToRun + jobAndCell.toTuple)
    
    def withCannotRun(jobAndCell: JobState): JobStatuses = copy(cannotRun = cannotRun + jobAndCell.toTuple)
  }
  
  object JobStatuses {
    val empty: JobStatuses = JobStatuses(Map.empty, Map.empty, 0, 0)
  }
}
