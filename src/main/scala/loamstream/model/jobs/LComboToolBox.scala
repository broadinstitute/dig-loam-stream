package loamstream.model.jobs

import loamstream.model.Tool
import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 3/28/2016.
  */
final case class LComboToolBox(boxes: Set[LToolBox]) extends LToolBox {
  override def toolToJobShot(tool: Tool): Shot[LJob] = Shot.findHit[LToolBox, LJob](boxes, _.toolToJobShot(tool))
}
