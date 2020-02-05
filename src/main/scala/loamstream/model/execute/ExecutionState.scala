package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.ValueBox
import loamstream.model.execute.ExecutionState.Fields

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
  
  private def cellAt(i: Int): ExecutionCell = fields.get(_.byJob.apply(i)._2)
  
  private def indexOf(job: LJob): Int = fields.get(_.index.apply(job))
  
  private def cellFor(job: LJob): ExecutionCell = fields.get(_ => cellAt(indexOf(job)))
  
  def isFinished: Boolean = fields.get { _ =>  
    justCells.forall(cell => cell.isFinished || cell.status.isCouldNotStart)
  }
  
  def statusOf(job: LJob): JobStatus = fields.get { _ =>
    requireKnown(job)
    
    cellFor(job).status
  }
  
  def updateJobs(): ExecutionState.JobStatuses = fields.get { _ =>
    val currentJobStatuses = jobStatuses
    
    val eligible = currentJobStatuses.readyToRun
    
    startRunning(eligible.keys)
    
    val toCancel = currentJobStatuses.cannotRun.keys
    
    markAs(toCancel, JobStatus.CouldNotStart)
    
    currentJobStatuses
  }
  
  def jobStatuses: ExecutionState.JobStatuses = fields.get { f =>  
    import f.{byJob => jobsToCells}
    
    val numRunning = jobsToCells.count { case (_, cell) => cell.isRunning }
    val numFinished = jobsToCells.count { case (_, cell) => cell.isFinished }
    
    val z = ExecutionState.JobStatuses.empty.copy(numRunning = numRunning, numFinished = numFinished)
    
    jobsToCells.foldLeft(z) { (acc, tuple) =>
      val (job, cell) = tuple
      
      def anyDepsStopExecution: Boolean = {
        val depCells = job.dependencies.iterator.map(_.job).map(cellFor) 
        
        cell.notStarted && depCells.exists(_.canStopExecution)
      }
      
      def canRun: Boolean = this.canRun(jobsToCells)(tuple)
      
      if(canRun) { acc.withRunnable(tuple) }
      else if(anyDepsStopExecution) { acc.withCannotRun(tuple) }
      else { acc }
    }
  }
  
  private def cellsFor(jobsToCells: Array[JobState])(jobs: Set[JobNode]): Array[ExecutionCell] = {
    val cells: Array[ExecutionCell] = Array.ofDim[ExecutionCell](jobs.size)
        
    var i = 0
    var j = 0
    
    while(i < jobsToCells.length) {
      val tuple = jobsToCells(i)
      
      if(jobs.contains(tuple._1)) {
        cells.update(j, tuple._2)
        
        j += 1
      }
      
      i += 1
    }
    
    cells
  }
  
  private[execute] def canRun(jobsToCells: Array[JobState])(tuple: JobState): Boolean = tuple match {
    case (job, cell) => cell.notStarted && {
      val deps = job.dependencies
      
      deps.isEmpty || {
        val depCells = cellsFor(jobsToCells)(deps)
        
        depCells.forall(_.isTerminal) && !depCells.exists(_.canStopExecution)
      }
    }
  }
  
  def eligibleToRun: Iterable[LJob] = fields.get { f =>
    import f.{byJob => jobsToCells}
    
    def canRun(t: JobState) = this.canRun(jobsToCells)(t)
    
    jobsToCells.collect { case t @ (job, _) if canRun(t) => job }
  }
  
  def startRunning(jobs: TraversableOnce[LJob]): Unit = transition(jobs, _.startRunning)
  
  def reRun(jobs: TraversableOnce[LJob]): Unit = transition(jobs, _.markAsRunnable)
  
  def markAs(jobs: TraversableOnce[LJob], jobStatus: JobStatus): Unit = transition(jobs, _.markAs(jobStatus))
  
  private def transition(jobs: TraversableOnce[LJob], doTransition: ExecutionCell => ExecutionCell): Unit = {
    if(jobs.nonEmpty) {
      val jobSet = jobs.toSet
      
      def doDoTransition(tuple: JobState): JobState = (tuple._1, doTransition(tuple._2))
      
      fields.foreach { f => 
        import f.{byJob => jobsToCells}
        
        requireAllKnown(jobSet)
        
        var i = 0
        
        while(i < jobsToCells.length) {
          val tuple = jobsToCells(i)
          
          if(jobSet.contains(tuple._1)) {
            jobsToCells.update(i, doDoTransition(tuple))
          }
          
          i += 1
        } 
      }
    }
  }
  
  def finish(job: LJob, execution: Execution): Unit = finish(job, execution.status, execution.result)
  
  def finish(job: LJob, status: JobStatus, jobResult: Option[JobResult] = None): Unit = {
    fields.foreach { f =>
      import f.{byJob => jobsToCells}
      
      requireKnown(job)
      
      val index = indexOf(job)
      
      val cell = cellFor(job)
      
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
      
      jobsToCells.update(index, (job -> transition(cell)))
      
      if(isTerminalFailure) {
        cancelSuccessors(f)(job)
      }
    }
  }
  
  private[execute] def cancelSuccessors(fields: Fields)(failedJob: LJob): Unit = {
    val successors = ExecuterHelpers.flattenTree(Set(failedJob), _.successors).toSet - failedJob
    
    markAs(successors.iterator.map(_.job), JobStatus.CouldNotStart)
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
  
  private final case class Fields(byJob: Array[JobState], index: Map[LJob, Int])
  
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
