package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.ValueBox
import loamstream.util.Iterators
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.Execution

/**
 * @author clint
 * Jan 24, 2020
 */
final class ExecutionState private (
    val maxRunsPerJob: Int,
    private[this] val byJob: ValueBox[Map[LJob, ExecutionCell]] = ValueBox(Map.empty)) {
  
  def isFinished: Boolean = byJob.get(_.values.forall(_.isFinished))
  
  def startEligibleJobs(): Iterable[LJob] = byJob.get { _ =>
    val eligible = eligibleToRun
    
    startRunning(eligible)
    
    eligible
  }
  
  def eligibleToRun: Iterable[LJob] = byJob.get { jobsToCells =>
    val canRun: ((LJob, ExecutionCell)) => Boolean = {
      case (job, cell) => cell.notStarted && {
        val deps = job.dependencies
        
        deps.isEmpty || {
          val depCells = jobsToCells.filterKeys(deps).values
          
          depCells.forall(_.isTerminal)
        }
      }
    }
    
    jobsToCells.filter(canRun).keys
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
      requireAllKnown(Set(job))
      
      def tooManyRuns: Boolean = jobsToCells(job).runCount >= maxRunsPerJob 
      
      val transition: ExecutionCell => ExecutionCell = {
        if(status.isFailure && tooManyRuns) { _.finishWith(JobStatus.FailedPermanently, jobResult) }
        else if (status.isFailure) { _.markAsRunnable }
        else {_.finishWith(status, jobResult) }
      }
      
      jobsToCells + (job -> transition(jobsToCells(job)))
    }
  }
  
  private def isKnown(job: LJob): Boolean = byJob.get(_.contains(job))
  
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
  def initialFor(executable: Executable, maxRunsPerJob: Int): ExecutionState = {
    val cellsByJob: Map[LJob, ExecutionCell] = Map.empty ++ {
      executable.allJobs.iterator.map(j => j -> ExecutionCell.initial)
    }
    
    new ExecutionState(maxRunsPerJob, ValueBox(cellsByJob))
  }
}
