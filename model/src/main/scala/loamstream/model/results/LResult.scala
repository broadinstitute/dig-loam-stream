package loamstream.model.results

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LSigTag
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
class LResult[+SigTag <: LSigTag, Inputs <: LPileCalls[_, _], Call <: LPileCall[SigTag, Inputs],
Tool <: LTool[SigTag, Inputs, Call]] {

}
