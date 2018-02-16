package loamstream.apps

import loamstream.model.execute.JobFilter
import loamstream.model.execute.Executable
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.NoOpJob
import loamstream.util.Iterators

/**
 * @author clint
 * Feb 13, 2018
 */
final class DryRunner(jobFilter: JobFilter) extends Loggable {
  
  def toBeRun(executable: Executable): Iterable[LJob] = {

    val seen = scala.collection.mutable.HashSet.empty[LJob]
    
    val markedAsRunnable = scala.collection.mutable.HashSet.empty[LJob]
    
    def alreadySeen(jn: JobNode) = seen(jn.job)
    
    def shouldBeRun(jn: JobNode) = markedAsRunnable(jn.job)
    
    def gatherJobs(jobNode: JobNode): Iterable[LJob] = {
      val job = jobNode.job

      if (alreadySeen(job)) {
        Nil
      } else {
        seen += job
        
        val pathToLeaf = {
          val inputsSortedById = jobNode.inputs.filterNot(alreadySeen).toSeq.sortBy(jobId)
          
          inputsSortedById.flatMap(gatherJobs)
        }

        val anyInputsNeedRunning = {
          def anyMarkedRunnable(nodes: Iterable[LJob]) = nodes.filter(markedAsRunnable).nonEmpty
          
          val inputJobs = jobNode.inputs.map(_.job)
          
          anyMarkedRunnable(inputJobs) || anyMarkedRunnable(pathToLeaf)
        }
        
        def shouldRunJob: Boolean = jobFilter.shouldRun(job)
        
        val shouldEmitJob = anyInputsNeedRunning || shouldRunJob
        
        val forJobNode = if (shouldEmitJob) Seq(job) else Nil

        markedAsRunnable ++= forJobNode
        
        pathToLeaf ++ forJobNode
      }
    }

    val rootJobNode = executable.plusNoOpRootJobIfNeeded.jobNodes.head

    import NoOpJob.isNoOpJob

    gatherJobs(rootJobNode).filterNot(isNoOpJob)
  }
  
  private def jobId(jn: JobNode) = jn.job.id
}

object DryRunner {
  sealed private trait GatherInputsResult
  
  case object ShouldRun
}
