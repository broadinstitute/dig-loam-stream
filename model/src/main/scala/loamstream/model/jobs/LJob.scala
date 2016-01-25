package loamstream.model.jobs

import util.shot.Shot

import scala.concurrent.Future

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LJob[T] {
  def inputs: Set[LJob[_]]

  def execute: Shot[Future[T]]
}
