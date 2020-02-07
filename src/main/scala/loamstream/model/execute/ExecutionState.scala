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
  
  private def justCells: Iterator[ExecutionCell] = fields.get { 
    _.byJob.iterator.collect { case (_, cell) => cell }
  }
  
  private def indexOf(job: LJob): Int = fields.get(_.index(job))
  
  private def cellAt(i: Int): ExecutionCell = fields.get(_.byJob.apply(i)._2)
  
  private def cellFor(job: LJob): ExecutionCell = fields.get(_ => cellAt(indexOf(job)))
  
  def isFinished: Boolean = fields.getWithTime("isFinished") { _ =>  
    justCells.forall(cell => cell.isFinished || cell.couldNotStart)
  }
  
  private[execute] def statusOf(job: LJob): JobStatus = fields.get { _ =>
    requireKnown(job)
    
    cellFor(job).status
  }
  
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
      
      val numRunning = jobsToCells.count { case (_, cell) => cell.isRunning }
      val numFinished = jobsToCells.count { case (_, cell) => cell.isFinished }
      
      val z = ExecutionState.JobStatuses.empty.copy(numRunning = numRunning, numFinished = numFinished)
      
      jobsToCells.foldLeft(z) { (acc, tuple) =>
        val (job, cell) = tuple
        
        def anyDepsStopExecution: Boolean = {
          def depCells = cellsFor(f)(job.dependencies.map(_.job)) 
          
          cell.notStarted && depCells.exists(_.canStopExecution)
        }
        
        def canRun: Boolean = this.canRun(f)(tuple)
        
        if(canRun) { acc.withRunnable(tuple) }
        else if(anyDepsStopExecution) { acc.withCannotRun(tuple) }
        else { acc }
      }
    }
  }
  
  private def cellsFor(f: Fields)(jobs: Set[JobNode]): Array[ExecutionCell] = {
    val indexes = jobs.iterator.map(_.job).map(f.index(_))
    
    val cells: Array[ExecutionCell] = Array.ofDim[ExecutionCell](jobs.size)
    
    indexes.map(i => f.byJob(i)._2).copyToArray(cells)
    
    cells
  }
  
  private[execute] def canRun(f: Fields)(tuple: JobState): Boolean = tuple match {
    case (job, cell) => cell.notStarted && {
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
      
      def doDoTransition(tuple: JobState): JobState = (tuple._1, doTransition(tuple._2))
      
      fields.foreach { f =>
        val jobIndices: Iterator[Int] = jobSet.iterator.map(f.index(_))
        
        jobIndices.foreach { jobIndex => 
          val tuple = f.byJob(jobIndex)
          
          f.byJob.update(jobIndex, doDoTransition(tuple))
        }
      }
    }
  }
  
  def finish(job: LJob, execution: Execution): Unit = finish(Seq(job -> execution))
  
  def finish(job: LJob, status: JobStatus, jobResult: Option[JobResult]): Unit = {
    finish(Seq((job, status, jobResult)))
  }
  
  def finish(results: Iterable[(LJob, Execution)]): Unit = {
    finish(results.iterator.map { case (job, execution) => (job, execution.status, execution.result) }) 
  }
  
  def finish(results: TraversableOnce[(LJob, JobStatus, Option[JobResult])])(implicit discriminator: Int = 42): Unit = {
    fields.foreachWithTime("finish()") { f =>
      results.foreach { case (job, status, jobResult) =>
        val index = indexOf(job)
        
        val cell = f.byJob(index)._2
        
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
        
        f.byJob.update(index, (job -> transition(cell)))
        
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
  
  private def isKnown(job: LJob): Boolean = fields.get(_.index.contains(job))
  
  private def requireKnown(job: LJob): Unit = require(isKnown(job), s"Expected job to be known: ${job}")
  
  private def requireAllKnown(jobs: Set[LJob]): Unit = {
    fields.foreach { f =>
      import f.{index => jobsToIndices}
      
      require(jobs.forall(isKnown), {
        val unknownJobs = jobs -- jobsToIndices.keys
      
        s"Expected all jobs to be known; at least one is not: ${jobs.iterator.next()}" 
      })
    }
  }
}

object ExecutionState {
  type JobState = (LJob, ExecutionCell)
  
  private final case class Fields(byJob: Array[JobState], index: Map[LJob, Int]) {
    def snapshot: Fields = Fields(copyOf(byJob), index)
  }
  
  private def copyOf(a: Array[JobState]): Array[JobState] = {
    val result: Array[JobState] = Array.ofDim(a.length)
    
    a.copyToArray(result)
    
    result
  }
  
  def initialFor(executable: Executable, maxRunsPerJob: Int): ExecutionState = {
    val cellsByJob: Array[JobState] = executable.allJobs.iterator.map(j => j -> ExecutionCell.initial).toArray
    
    val indicesByJob: Map[LJob, Int] = {
      Map.empty ++ cellsByJob.iterator.zipWithIndex.map { case ((job, _), i) => (job -> i) } 
    }
    
    new ExecutionState(maxRunsPerJob, ValueBox(Fields(cellsByJob, indicesByJob)))
  }
  
  final case class JobStatuses(
      readyToRun: Map[LJob, ExecutionCell], 
      cannotRun: Map[LJob, ExecutionCell],
      numRunning: Int,
      numFinished: Int) {
    
    def withRunnable(jobAndCell: JobState): JobStatuses = copy(readyToRun = readyToRun + jobAndCell)
    
    def withCannotRun(jobAndCell: JobState): JobStatuses = copy(cannotRun = cannotRun + jobAndCell)
  }
  
  object JobStatuses {
    val empty: JobStatuses = JobStatuses(Map.empty, Map.empty, 0, 0)
  }
}
