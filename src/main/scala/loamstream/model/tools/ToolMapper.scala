package loamstream.model.tools

import loamstream.model.calls.LPileCall
import loamstream.model.tags.LPileTag
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait ToolMapper {

  def findTool[Tag <: LPileTag[_, _], Call <: LPileCall[Tag]]: Shot[LTool[Tag, Call]]

}
