package loamstream.model.results

import loamstream.model.calls.{LKeys, LPileCall}
import loamstream.model.recipes.LPileCalls
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
class LResult[Keys <: LKeys[_, _], Inputs <: LPileCalls[_, _],
Call <: LPileCall[Keys, Inputs], Tool <: LTool[Keys, Inputs, Call]] {

}
