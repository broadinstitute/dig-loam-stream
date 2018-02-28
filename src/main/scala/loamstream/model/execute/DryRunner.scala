package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.model.jobs.JobNode
import scala.collection.Seq
import loamstream.util.CanBeClosed
import java.io.PrintWriter
import java.io.FileWriter
import java.nio.file.Path
import java.time.Instant

/**
 * @author clint
 * Feb 13, 2018
 */
object DryRunner extends Loggable {
  
  def toBeRun(jobFilter: JobFilter, executable: Executable): Iterable[LJob] = {

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

    executable.jobNodes.toSeq.sortBy(jobId).flatMap(gatherJobs)
  }
  
  private def jobId(jn: JobNode) = jn.job.id
}
