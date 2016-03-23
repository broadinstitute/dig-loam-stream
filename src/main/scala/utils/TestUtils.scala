package utils

import loamstream.model.jobs.LJob
import loamstream.util.shot.{Hit, Shot}

/**
  * LoamStream
  * Created by oliverr on 3/4/2016.
  */
object TestUtils {

  def assertSomeAndGet[A](option: Option[A]): A = {
    assert(option.nonEmpty)
    option.get
  }

  def isHitOfSetOfOne(shot: Shot[Set[LJob]]): Boolean = {
    shot match {
      case Hit(jobs) => jobs.size == 1
      case _ => false
    }
  }

}
