package loamstream.apps.minimal

import loamstream.model.execute.{LExecutable, LExecuter}
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.shot.{Miss, Shot, Hit}
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
object MiniExecuter extends LExecuter {
  override def execute(executable: LExecutable): Map[LJob, Shot[Result]] = {
    var jobsWaiting: Set[LJob] = executable.jobs
    var results: Map[LJob, Shot[Result]] = Map.empty
    var weAreStuck = false
    while (jobsWaiting.nonEmpty && !weAreStuck) {
      val jobsReady = jobsWaiting.filter(_.inputs.forall({ inputJob => results.get(inputJob) match {
        case Some(Hit(result)) => result match {
          case success: LJob.Success => true
          case failure: LJob.Failure => weAreStuck = true; false
        }
        case Some(_) => weAreStuck = true; false
        case None => false
      }
      }))
      if(jobsReady.nonEmpty) {
        val job = jobsReady.head
        job.execute match {
          case Hit(resultFuture) =>
            val result = Await.result(resultFuture, Duration.Inf)
            results += (job -> Hit(result))
          case miss:Miss => results += (job -> miss)
        }
        jobsWaiting -= job
      }
    }
    results
  }
}
