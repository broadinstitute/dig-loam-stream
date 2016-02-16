package loamstream.model.jobs

import loamstream.model.jobs.tools.LTool
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object LToolBox {
//  case class LToolBag(tools:Set[LTool[_]]) extends LToolBox {
//    override def toolsFor(kind: LKind): Set[LTool[_]] = ??? // TODO
//  }
}
trait LToolBox {
  def toolsFor(kind: LKind): Set[LTool[_]]
}
