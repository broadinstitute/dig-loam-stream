package loamstream.model.jobs

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LPileTag
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
case class LJob[Tag <: LPileTag[_, _], Inputs <: LPileCalls[_, _, _],
Call <: LPileCall[Tag, Inputs]](call: Call, tool: LTool[Tag, Inputs, Call]) {

}
