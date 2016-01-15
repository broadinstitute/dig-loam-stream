package loamstream.model.results

import loamstream.model.calls.{LKeys, LPileCall}
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LSigTag
import loamstream.model.tools.LTool
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LExecuter {

  def execute[Keys <: LKeys[_, _], SigTag <: LSigTag, Inputs <: LPileCalls[_, _],
  Call <: LPileCall[Keys, SigTag, Inputs],
  Tool <: LTool[Keys, SigTag, Inputs, Call]]: Shot[LResult[Keys, SigTag, Inputs, Call, Tool]]

}
