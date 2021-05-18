package loamstream.model.jobs

import org.scalatest.FunSuite

import scala.collection.immutable.Seq
import loamstream.util.Maps

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  
  private def count[A](as: Seq[A]): Map[A, Int] = {
    import Maps.Implicits.MapOps
    
    as.groupBy(identity).strictMapValues(_.size)
  }
  
  //TODO: Lame :(
  private def toLJob(lj: LocalJob): LJob = lj
  //TODO: Lame :(
  private def toJobNode(j: LJob): JobNode = j
  //TODO: Lame :(
  private def toLocalJob(j: LJob): LocalJob = j.asInstanceOf[LocalJob]
  
  //TODO
}
