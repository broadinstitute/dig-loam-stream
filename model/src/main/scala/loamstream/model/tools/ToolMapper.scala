package loamstream.model.tools

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LSigTag
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait ToolMapper {
  def findTool[SigTag <: LSigTag, Inputs <: LPileCalls[_, _],
  Call <: LPileCall[SigTag, Inputs]]: Shot[LTool[SigTag, Inputs, Call]]
}
