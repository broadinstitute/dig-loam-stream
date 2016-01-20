package loamstream.model.tools

import loamstream.model.calls.LPileCall
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait ToolMapper {
  def findTool[Call <: LPileCall]: Shot[LTool[Call]]
}
