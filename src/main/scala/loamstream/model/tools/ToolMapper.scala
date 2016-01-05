package loamstream.model.tools

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LPileTag
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait ToolMapper {

  def findTool[Tag <: LPileTag[_, _], Inputs <: LPileCalls[_, _, _],
  Call <: LPileCall[Tag, Inputs]]: Shot[LTool[Tag, Inputs, Call]]

}
