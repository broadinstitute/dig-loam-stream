package loamstream.model.results

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.{LSemTag, LSigTag}
import loamstream.model.tools.LTool
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LExecuter {

  def execute[SigTag <: LSigTag, SemTag <: LSemTag, Inputs <: LPileCalls[_, _],
  Call <: LPileCall[SigTag, SemTag, Inputs], Tool <: LTool[SigTag, SemTag, Inputs, Call]]:
  Shot[LResult[SigTag, SemTag, Inputs, Call, Tool]]

}
