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
    
    def gatherJobs(jobNode: JobNode): Iterable[LJob] = {

      def alreadySeen(jn: JobNode) = seen(jn.job)

      def jobId(jn: JobNode) = jn.job.id
      
      val job = jobNode.job

      if (seen(job)) {
        Nil
      } else {
        seen += job
        
        val forInputs = {
          jobNode.inputs.filterNot(alreadySeen).toSeq.sortBy(jobId).flatMap(gatherJobs).map(_.job)
        }

        val forJobNode = if (jobFilter.shouldRun(job)) Seq(job) else Nil

        forInputs ++ forJobNode
      }
    }

    val rootJobNode = executable.plusNoOpRootJobIfNeeded.jobNodes.head

    import NoOpJob.isNoOpJob

    gatherJobs(rootJobNode).filterNot(isNoOpJob)
  }
}
