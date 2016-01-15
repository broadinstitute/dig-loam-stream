package loamstream.model.tools

import loamstream.model.calls.{LKeys, LPileCall}
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LSigTag
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait ToolMapper {
  def findTool[Keys <: LKeys[_, _], SigTag <: LSigTag, Inputs <: LPileCalls[_, _],
  Call <: LPileCall[Keys, SigTag, Inputs]]: Shot[LTool[Keys, SigTag, Inputs, Call]]
}
