package loamstream.model.results

import loamstream.model.calls.{LKeys, LPileCall}
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LSigTag
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
class LResult[Keys <: LKeys[_, _], +SigTag <: LSigTag, Inputs <: LPileCalls[_, _],
Call <: LPileCall[Keys, SigTag, Inputs], Tool <: LTool[Keys, SigTag, Inputs, Call]] {

}
