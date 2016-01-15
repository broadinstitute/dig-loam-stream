package loamstream.model.tools

import loamstream.model.calls.{LKeys, LPileCall}
import loamstream.model.recipes.LPileCalls
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait ToolMapper {
  def findTool[Keys <: LKeys[_, _], Inputs <: LPileCalls[_, _],
  Call <: LPileCall[Keys, Inputs]]: Shot[LTool[Keys, Inputs, Call]]
}
