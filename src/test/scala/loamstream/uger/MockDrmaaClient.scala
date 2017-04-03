package loamstream.uger

import scala.util.Try
import scala.concurrent.duration.Duration
import java.nio.file.Path

import loamstream.conf.UgerConfig
import loamstream.util.ValueBox
import loamstream.util.Maps

/**
 * @author clint
 * date: Jul 6, 2016
 */
final case class MockDrmaaClient(private val toReturn: Map[String, Seq[Try[UgerStatus]]]) extends DrmaaClient {
  import Maps.Implicits._
  
  private val remaining: ValueBox[Map[String, Seq[Try[UgerStatus]]]] = {
    ValueBox(toReturn.strictMapValues(_.init))
  }
  
  private val terminalStates = toReturn.map { case (jobId, statuses) => (jobId, statuses.last) }
  
  val params: ValueBox[Seq[(String, Duration)]] = ValueBox(Vector.empty)
  
  override def submitJob(
                          ugerConfig: UgerConfig,
                          pathToScript: Path,
                          jobName: String,
                          numTasks: Int): DrmaaClient.SubmissionResult = ???

  override def statusOf(jobId: String): Try[UgerStatus] = waitFor(jobId, Duration.Zero)

  override def waitFor(jobId: String, timeout: Duration): Try[UgerStatus] = {
    params.mutate(_ :+ (jobId -> timeout))
    
    remaining.getAndUpdate { leftToReturn => 
      val statuses = leftToReturn.get(jobId).filter(_.nonEmpty)
      
      val (nextStatus, remainingStatuses) = statuses match {
        case Some(sts) => (sts.head, sts.tail)
        case None => (terminalStates(jobId), Nil)
      }
      
      val nextMap = leftToReturn.updated(jobId, remainingStatuses)
      
      nextMap -> nextStatus
    }
  }

  override def stop(): Unit = ()
  
  override def killJob(jobId: String): Unit = ???
  
  override def killAllJobs(): Unit = ???
}
