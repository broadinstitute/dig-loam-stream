package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.ValueBox
import loamstream.util.Iterators
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.Execution
import loamstream.util.Loggable

/**
 * @author clint
 * Jan 24, 2020
 */
final class ExecutionState private (
    val maxRunsPerJob: Int,
    private[this] val byJob: ValueBox[Map[LJob, ExecutionCell]] = ValueBox(Map.empty)) extends Loggable {
  
  private[execute] def allJobs: Iterable[LJob] = byJob.get(_.keys)
  
  def isFinished: Boolean = byJob.get { jobsToCells => 
    jobsToCells.values.forall(cell => cell.isFinished || cell.status.isCouldNotStart)
  }
  
  private[execute] def statusOf(job: LJob): JobStatus = {
    requireKnown(job)
    
    byJob.get(_.apply(job).status)
  }
  
  def updateJobs(): ExecutionState.JobStatuses = byJob.get { _ =>
    val currentJobStatuses = jobStatuses
    
    val eligible = currentJobStatuses.readyToRun
    
    startRunning(eligible.keys)
    
    val cannotRun = currentJobStatuses.cannotRun.keys
    
    markAs(cannotRun, JobStatus.CouldNotStart)
    
    currentJobStatuses
  }
  
  def jobStatuses: ExecutionState.JobStatuses = byJob.get { jobsToCells =>
    val z = ExecutionState.JobStatuses.empty
    
    jobsToCells.foldLeft(z) { (acc, tuple) =>
      val (job, cell) = tuple
      
      def anyDepsStopExecution: Boolean = {
        val depCells = jobsToCells.filterKeys(job.dependencies).values
        
        cell.notStarted && depCells.exists(_.canStopExecution)
      }
      
      def canRun: Boolean = this.canRun(jobsToCells)(tuple)
      
      if(canRun) { acc.withRunnable(tuple) }
      else if(anyDepsStopExecution) { acc.withCannotRun(tuple) }
      else { acc }
    }
  }
  
  import ExecutionState.Availability
  
  private[execute] def determineAvailability
      (jobsToCells: Map[LJob, ExecutionCell])(tuple: (LJob, ExecutionCell)): Availability = tuple match {
    
    case (job, cell) => {
      val deps = job.dependencies.map(_.job)
      
      if(cell.notStarted && deps.isEmpty) { Availability.ReadyToRun }
      else if(cell.notStarted) {
        val depCells = jobsToCells.filterKeys(deps).values
      
        val anyDepCanStopExecution = depCells.exists(_.canStopExecution)
        
        if(anyDepCanStopExecution) { Availability.CannotRun }
        else if(depCells.forall(_.isTerminal)) { Availability.ReadyToRun }
        else { Availability.NotYet }
      } else {
        Availability.NotYet
      }
    }
  }
  
  private[execute] def canRun(jobsToCells: Map[LJob, ExecutionCell])(tuple: (LJob, ExecutionCell)): Boolean = tuple match {
    case (job, cell) => cell.notStarted && {
      val deps = job.dependencies
      
      deps.isEmpty || {
        val depCells = jobsToCells.filterKeys(deps).values
        
        depCells.forall(_.isTerminal) && !depCells.exists(_.canStopExecution)
      }
    }
  }
  
  def eligibleToRun: Iterable[LJob] = byJob.get { jobsToCells =>
    def canRun(t: (LJob, ExecutionCell)) = this.canRun(jobsToCells)(t)
    
    jobsToCells.collect { case t @ (job, _) if canRun(t) => job }
  }
  
  def startRunning(jobs: Iterable[LJob]): Unit = transition(jobs, _.startRunning)
  
  def reRun(jobs: Iterable[LJob]): Unit = transition(jobs, _.markAsRunnable)
  
  def markAs(jobs: Iterable[LJob], jobStatus: JobStatus): Unit = transition(jobs, _.markAs(jobStatus))
  
  private def transition(jobs: Iterable[LJob], doTransition: ExecutionCell => ExecutionCell): Unit = {
    if(jobs.nonEmpty) {
      val jobSet = jobs.toSet
      
      byJob.mutate { jobsToCells =>
        requireAllKnown(jobSet)
        
        jobsToCells ++ jobsToCells.filterKeys(jobSet).mapValues(doTransition)
      }
    }
  }
  
  def finish(job: LJob, execution: Execution): Unit = finish(job, execution.status, execution.result)
  
  def finish(job: LJob, status: JobStatus, jobResult: Option[JobResult] = None): Unit = {
    byJob.mutate { jobsToCells =>
      requireKnown(job)
      
      lazy val runCount: Int = jobsToCells(job).runCount
      
      lazy val tooManyRuns: Boolean = runCount >= maxRunsPerJob 
      
      val transition: ExecutionCell => ExecutionCell = {
        if(status.isFailure && tooManyRuns) { 
          debug(s"Restarting $job ? NO (job has run $runCount times, max is $maxRunsPerJob)")
          
          _.finishWith(JobStatus.FailedPermanently, jobResult) }
        else if (status.isFailure) {
          debug(s"Restarting $job ? YES (job has run $runCount times, max is $maxRunsPerJob)")
          
          _.markAsRunnable 
        }
        else {_.finishWith(status, jobResult) }
      }
      
      jobsToCells + (job -> transition(jobsToCells(job)))
    }
  }
  
  private def isKnown(job: LJob): Boolean = byJob.get(_.contains(job))
  
  private def requireKnown(job: LJob): Unit = require(isKnown(job), s"Expected job to be known: ${job}")
  
  private def requireAllKnown(jobs: Set[LJob]): Unit = {
    byJob.foreach { jobsToCells =>
      require(jobs.forall(isKnown), {
        val unknownJobs = jobs -- jobsToCells.keys
      
        s"Expected all jobs to be known; at least one is not: ${jobs.iterator.next()}" 
      })
    }
  }
}

object ExecutionState {
  sealed trait Availability 
  
  object Availability {
    final case object ReadyToRun extends Availability
    final case object CannotRun extends Availability
    final case object NotYet extends Availability
  }
  
  def initialFor(executable: Executable, maxRunsPerJob: Int): ExecutionState = {
    val cellsByJob: Map[LJob, ExecutionCell] = Map.empty ++ {
      executable.allJobs.iterator.map(j => j -> ExecutionCell.initial)
    }
    
    new ExecutionState(maxRunsPerJob, ValueBox(cellsByJob))
  }
  
  final case class JobStatuses(
      readyToRun: Map[LJob, ExecutionCell], 
      cannotRun: Map[LJob, ExecutionCell]) {
    
    def withRunnable(jobAndCell: (LJob, ExecutionCell)): JobStatuses = copy(readyToRun = readyToRun + jobAndCell)
    
    def withCannotRun(jobAndCell: (LJob, ExecutionCell)): JobStatuses = copy(cannotRun = cannotRun + jobAndCell)
  }
  
  object JobStatuses {
    val empty: JobStatuses = JobStatuses(Map.empty, Map.empty)
  }
}
