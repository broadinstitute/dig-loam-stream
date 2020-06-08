package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.AsyncLocalChunkRunner

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  
  import JobStatus._
  import loamstream.TestHelpers.waitFor
  import loamstream.util.Observables.Implicits._

  private def count[A](as: Seq[A]): Map[A, Int] = as.groupBy(identity).mapValues(_.size).toMap
  
  //TODO: Lame :(
  private def toLJob(lj: LocalJob): LJob = lj
  //TODO: Lame :(
  private def toJobNode(j: LJob): JobNode = j
  //TODO: Lame :(
  private def toLocalJob(j: LJob): LocalJob = j.asInstanceOf[LocalJob]
  
  //TODO
}
