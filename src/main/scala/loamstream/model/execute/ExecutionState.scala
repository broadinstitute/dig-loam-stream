package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.ValueBox
import loamstream.util.TimeUtils
import loamstream.util.Sequence
import cats.kernel.Eq
import scala.collection.compat._

/**
 * @author clint
 * Jan 24, 2020
 * 
 * The state of the execution of a LoamStream pipeline.  This is intended to be the single point of truth when
 * executing.  
 * The data structure that tracks the state of all the jobs being executed by LoamStream on a given run is an 
 * array of JobStates ((Job, ExecutionCell) tuples).  An array is used for a few reasons, mostly related to 
 * performance.  Initally, I tried a Map[LJob, ExecutionCell], but while this allowed fast lookups of the state 
 * of a job, iterating through the Map to find runnable jobs (something LS does very frequently) was too slow. 
 * Updates to lots of jobs in the immutable Map (the norm when running large pipelines) also produced a large 
 * amount of GC pressure.
 * 
 * A mutable array addresses both of these: iterating over an array is as fast as it gets on the JVM, and only
 * making new ExecutionCells on job state changes instead of new Maps substantially reduced the amount of
 * allocations and GC activity.  Together, these factors increased performance a lot.
 * 
 * Looking up JobStates by jobs, and finding the subset of JobStates for a given bunch of jobs is made fast
 * (enough) by pairing the Array[JobState] with an index, a Map[LJob, Int], mapping jobs to their positions in the
 * array.
 */
final class ExecutionState private (
    val maxRunsPerJob: Int,
    private[this] val jobStatesBox: ValueBox[Array[JobExecutionState]],
    //NB: Profiler-guided optimization: Use a java.util.Map for slightly faster performance.  
    //The index field will be used /a lot/.
    index: java.util.Map[LJob, Int]) extends Loggable {
  
  def size: Int = jobStatesBox.get(_.size)
  
  private[execute] def allJobs: Iterable[LJob] = {
    import scala.collection.JavaConverters._
    
    index.keySet.iterator.asScala.toIterable
  }
  
  /**
   * Are all jobs "done"?  (Ie, finished or deliberately never started) 
   */
  def isFinished: Boolean = jobStatesBox.get { jobStates =>  
    jobStates.forall(cell => cell.isFinished || cell.couldNotStart)
  }
  
  private[execute] def statusOf(job: LJob): JobStatus = {
    def cellFor(job: LJob)(jobStates: Array[JobExecutionState]): JobExecutionState = jobStates.apply(index.get(job))
    
    jobStatesBox.get(cellFor(job)).status
  }
  
  private[execute] def snapshot(): Array[JobExecutionState] = jobStatesBox.get(ExecutionState.copyOf)
  
  /**
   * Returns a view of the current state of all jobs, useful to Executers. (The result of jobStatuses())
   * 
   * Before that, this method obtains that view, uses it to determine which jobs are runnable, and marks them
   * as running.  Jobs that will never be runnable are marked CouldNotStart.  RxExecuter invokes this method 
   * repeatedly to get new lists of jobs to run.
   */
  def updateJobs(): ExecutionState.JobStatuses = jobStatesBox.get { _ =>
    TimeUtils.time(s"updateJobs()", trace(_)) {
      val currentJobStatuses = jobStatuses
      
      val eligible = currentJobStatuses.readyToRun.iterator.map(_.job)
      
      TimeUtils.time(s"startRunning()", trace(_)) {
        startRunning(eligible)
      }
      
      val toCancel = currentJobStatuses.cannotRun.iterator.map(_.job)
      
      TimeUtils.time(s"markAs(CouldNotStart)", trace(_)) { 
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
    val jobStates = snapshot()
 
    TimeUtils.time("Computing JobStatuses", trace(_)) {
      val numRunning = jobStates.count(_.isRunning)
      val numFinished = jobStates.count(_.isFinished)
      
      val z = ExecutionState.JobStatuses.empty.copy(numRunning = numRunning, numFinished = numFinished)
      
      jobStates.foldLeft(z) { (acc, jobState) =>
        import jobState.job
        
        def anyDepsStopExecution: Boolean = {
          //Profiler-guided optimization: mapping over a Set is slow enough that we convert to a Seq first here.
          def depStates = statesFor(jobStates)(job.dependencies.to(Seq).map(_.job)) 
          
          jobState.notStarted && depStates.exists(_.canStopExecution)
        }
        
        def canRun: Boolean = this.canRun(jobStates)(jobState)
        
        if(canRun) { acc.withRunnable(jobState) }
        else if(anyDepsStopExecution) { acc.withCannotRun(jobState) }
        else { acc }
      }
    }
  }
  
  /**
   * Obtains the JobExecutionState for a given bunch of jobs, given the states of all jobs.
   * (Returns an array for fast iteration (profiling turned this up). 
   */
  private def statesFor(jobStates: Array[JobExecutionState])(jobs: Iterable[JobNode]): Array[JobExecutionState] = {
    val indexes = jobs.iterator.map(_.job).map(index.get(_))
    
    val cells: Array[JobExecutionState] = Array.ofDim[JobExecutionState](jobs.size)
    
    indexes.map(i => jobStates(i)).copyToArray(cells)
    
    cells
  }
  
  /**
   * Can a job run, given its current state, and the states of all other jobs.
   */
  private[execute] def canRun(jobStates: Array[JobExecutionState])(jobState: JobExecutionState): Boolean = {
    import jobState.job
    
    jobState.notStarted && {
      val deps = job.dependencies
      
      deps.isEmpty || {
        val depCells = statesFor(jobStates)(deps)
        
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
  
  private def transition(jobs: TraversableOnce[LJob], doTransition: JobExecutionState => JobExecutionState): Unit = {
    if(jobs.nonEmpty) {
      val jobSet = jobs.toSet
      
      jobStatesBox.foreach { jobStates =>
        val jobIndices: Iterator[Int] = jobSet.iterator.map(index.get(_))
        
        jobIndices.foreach { jobIndex => 
          val jobState = jobStates(jobIndex)
          
          jobStates(jobIndex) = doTransition(jobState)
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
      jobStates <- jobStatesBox
      (job, status) <- results
    } {
      val jobIndex = index.get(job)
        
      val jobState = jobStates(jobIndex)
      
      lazy val runCount: Int = jobState.runCount
      
      lazy val tooManyRuns: Boolean = runCount >= maxRunsPerJob 
      
      lazy val isTerminalFailure: Boolean = status.isFailure && tooManyRuns
      
      val transition: JobExecutionState => JobExecutionState = {
        if(isTerminalFailure) { 
          debug(s"Restarting $job ? NO (job has run $runCount times, max is $maxRunsPerJob)")
          
          _.finishWith(JobStatus.FailedPermanently) 
        } else if (status.isFailure) {
          debug(s"Restarting $job ? YES (job has run $runCount times, max is $maxRunsPerJob)")
          
          _.markAsRunnable 
        }
        else {_.finishWith(status) }
      }
      
      jobStates(jobIndex) = transition(jobState)
      
      if(isTerminalFailure || status.isCanceled) {
        cancelSuccessors(job)
      }
    }
  }
  
  /**
   * Cancel all the jobs that depend on this job, and the ones that depend on them, etc, by
   * marking them as CouldNotStart.
   */
  private[execute] def cancelSuccessors(failedJob: LJob): Unit = {
    TimeUtils.time(s"Cancelling successors for failed job with id ${failedJob.id}", debug(_)) {
      val successors = ExecuterHelpers.flattenTree(Set(failedJob), _.successors).toSet - failedJob
      
      markAs(successors.iterator.map(_.job), JobStatus.CouldNotStart)
    }
  }
}

object ExecutionState {
  private def copyOf(a: Array[JobExecutionState]): Array[JobExecutionState] = {
    val result: Array[JobExecutionState] = Array.ofDim(a.length)
    
    a.copyToArray(result)
    
    result
  }
  
  def initialFor(executable: Executable, maxRunsPerJob: Int): ExecutionState = {
    val jobStates: Array[JobExecutionState] = executable.allJobs.iterator.map(JobExecutionState.initialFor).toArray
    
    val indicesByJob: java.util.Map[LJob, Int] = {
      val result: java.util.Map[LJob, Int] = new java.util.HashMap 
      
      val values = Map.empty ++ jobStates.iterator.zipWithIndex.map { case (jobState, i) => (jobState.job -> i) }
      
      import scala.collection.JavaConverters._
      
      result.putAll(values.asJava)
      
      result
    }
    
    new ExecutionState(maxRunsPerJob, ValueBox(jobStates), indicesByJob)
  }
  
  final case class JobStatuses(
      readyToRun: Set[JobExecutionState], 
      cannotRun: Set[JobExecutionState],
      numRunning: Int,
      numFinished: Int) {
    
    def withRunnable(state: JobExecutionState): JobStatuses = copy(readyToRun = readyToRun + state)
    
    def withCannotRun(state: JobExecutionState): JobStatuses = copy(cannotRun = cannotRun + state)
  }
  
  object JobStatuses {
    val empty: JobStatuses = JobStatuses(Set.empty, Set.empty, 0, 0)
    
    implicit val eqJobStatuses: Eq[JobStatuses] = Eq.fromUniversalEquals
  }
}
