package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.ValueBox
import loamstream.model.execute.ExecutionState.Fields
import loamstream.util.TimeUtils
import loamstream.util.Sequence

/**
 * @author clint
 * Jan 24, 2020
 */
final class ExecutionState private (
    val maxRunsPerJob: Int,
    private[this] val fields: ValueBox[ExecutionState.Fields]) extends Loggable {
  
  import ExecutionState.JobState
  
  def size: Int = fields.get(_.byJob.length)
  
  private[execute] def allJobs: Iterable[LJob] = fields.get(_.index.keys)
  
  private def justCells: Iterator[ExecutionCell] = fields.get(_.byJob.iterator.map(_.cell))
  
  def isFinished: Boolean = fields.getWithTime("isFinished") { _ =>  
    justCells.forall(cell => cell.isFinished || cell.couldNotStart)
  }
  
  private[execute] def statusOf(job: LJob): JobStatus = fields.get(_.cellFor(job).status)
  
  private[this] val sequence: Sequence[Int] = Sequence()
  
  def updateJobs(): ExecutionState.JobStatuses = fields.get { _ =>
    val i = sequence.next()
    
    TimeUtils.time(s"updateJobs($i)", debug(_)) {
      val currentJobStatuses = jobStatuses
      
      val eligible = currentJobStatuses.readyToRun
      
      TimeUtils.time(s"startRunning($i)", debug(_)) {
        startRunning(eligible.keys)
      }
      
      val toCancel = currentJobStatuses.cannotRun.keys
      
      TimeUtils.time(s"markAs(CouldNotStart)($i)", debug(_)) { 
        markAs(toCancel, JobStatus.CouldNotStart)
      }
      
      currentJobStatuses
    }
  }

  def jobStatuses: ExecutionState.JobStatuses = { 
    val f = fields.valueWithTime("jobStatuses").snapshot      
  
    TimeUtils.time("Computing JobStatuses", debug(_)) {
      import f.{byJob => jobsToCells}
      
      val numRunning = jobsToCells.count(_.cell.isRunning)
      val numFinished = jobsToCells.count(_.cell.isFinished)
      
      val z = ExecutionState.JobStatuses.empty.copy(numRunning = numRunning, numFinished = numFinished)
      
      jobsToCells.foldLeft(z) { (acc, jobState) =>
        import jobState.{job, cell}
        
        def anyDepsStopExecution: Boolean = {
          def depCells = cellsFor(f)(job.dependencies.map(_.job)) 
          
          cell.notStarted && depCells.exists(_.canStopExecution)
        }
        
        def canRun: Boolean = this.canRun(f)(jobState)
        
        if(canRun) { acc.withRunnable(jobState) }
        else if(anyDepsStopExecution) { acc.withCannotRun(jobState) }
        else { acc }
      }
    }
  }
  
  private def cellsFor(f: Fields)(jobs: Set[JobNode]): Array[ExecutionCell] = {
    val indexes = jobs.iterator.map(_.job).map(f.index(_))
    
    val cells: Array[ExecutionCell] = Array.ofDim[ExecutionCell](jobs.size)
    
    indexes.map(i => f.byJob(i).cell).copyToArray(cells)
    
    cells
  }
  
  private[execute] def canRun(f: Fields)(jobState: JobState): Boolean = {
    import jobState.{job, cell}
    
    cell.notStarted && {
      val deps = job.dependencies
      
      deps.isEmpty || {
        val depCells = cellsFor(f)(deps)
        
        depCells.forall(_.isTerminal) && !depCells.exists(_.canStopExecution)
      }
    }
  }
  
  def startRunning(jobs: TraversableOnce[LJob]): Unit = transition(jobs, _.startRunning)
  
  def reRun(jobs: TraversableOnce[LJob]): Unit = transition(jobs, _.markAsRunnable)
  
  def markAs(jobs: TraversableOnce[LJob], jobStatus: JobStatus): Unit = transition(jobs, _.markAs(jobStatus))
  
  private def transition(jobs: TraversableOnce[LJob], doTransition: ExecutionCell => ExecutionCell): Unit = {
    if(jobs.nonEmpty) {
      val jobSet = jobs.toSet
      
      def doDoTransition(jobState: JobState): JobState = jobState.transformCell(doTransition)
      
      fields.foreach { f =>
        val jobIndices: Iterator[Int] = jobSet.iterator.map(f.index(_))
        
        jobIndices.foreach { jobIndex => 
          val tuple = f.byJob(jobIndex)
          
          f.byJob(jobIndex) = doDoTransition(tuple)
        }
      }
    }
  }
  
  def finish(results: Iterable[(LJob, Execution)]): Unit = {
    finish(results.iterator.map { case (job, execution) => (job, execution.status, execution.result) }) 
  }
  
  def finish(job: LJob, status: JobStatus, jobResult: Option[JobResult]): Unit = {
    finish(Seq((job, status, jobResult)))
  }
  
  def finish(results: TraversableOnce[(LJob, JobStatus, Option[JobResult])])(implicit discriminator: Int = 1): Unit = {
    fields.foreachWithTime("finish()") { f =>
      results.foreach { case (job, status, jobResult) =>
        val jobIndex = f.index(job)
        
        val cell = f.byJob(jobIndex).cell
        
        lazy val runCount: Int = cell.runCount
        
        lazy val tooManyRuns: Boolean = runCount >= maxRunsPerJob 
        
        lazy val isTerminalFailure: Boolean = status.isFailure && tooManyRuns
        
        val transition: ExecutionCell => ExecutionCell = {
          if(isTerminalFailure) { 
            debug(s"Restarting $job ? NO (job has run $runCount times, max is $maxRunsPerJob)")
            
            _.finishWith(JobStatus.FailedPermanently, jobResult) 
          } else if (status.isFailure) {
            debug(s"Restarting $job ? YES (job has run $runCount times, max is $maxRunsPerJob)")
            
            _.markAsRunnable 
          }
          else {_.finishWith(status, jobResult) }
        }
        
        f.byJob(jobIndex) = JobState(job, transition(cell))
        
        if(isTerminalFailure) {
          cancelSuccessors(f)(job)
        }
      }
    }
  }
  
  private[execute] def cancelSuccessors(fields: Fields)(failedJob: LJob): Unit = {
    TimeUtils.time(s"Cancelling successors for failed job with id ${failedJob.id}", debug(_)) {
      val successors = ExecuterHelpers.flattenTree(Set(failedJob), _.successors).toSet - failedJob
      
      markAs(successors.iterator.map(_.job), JobStatus.CouldNotStart)
    }
  }
}

object ExecutionState {
  final case class JobState(job: LJob, cell: ExecutionCell) {
    def toTuple: (LJob, ExecutionCell) = (job, cell)
    
    def transformCell(f: ExecutionCell => ExecutionCell): JobState = copy(cell = f(cell))
  }
  
  object JobState {
    def initialFor(job: LJob): JobState = JobState(job, ExecutionCell.initial)
  }
  
  private final case class Fields(byJob: Array[JobState], index: Map[LJob, Int]) {
    def snapshot: Fields = Fields(copyOf(byJob), index)
    
    def cellFor(job: LJob): ExecutionCell = byJob(index(job)).cell
  }
  
  private def copyOf(a: Array[JobState]): Array[JobState] = {
    val result: Array[JobState] = Array.ofDim(a.length)
    
    a.copyToArray(result)
    
    result
  }
  
  def initialFor(executable: Executable, maxRunsPerJob: Int): ExecutionState = {
    val cellsByJob: Array[JobState] = executable.allJobs.iterator.map(JobState.initialFor).toArray
    
    val indicesByJob: Map[LJob, Int] = {
      Map.empty ++ cellsByJob.iterator.zipWithIndex.map { case (jobState, i) => (jobState.job -> i) } 
    }
    
    new ExecutionState(maxRunsPerJob, ValueBox(Fields(cellsByJob, indicesByJob)))
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
