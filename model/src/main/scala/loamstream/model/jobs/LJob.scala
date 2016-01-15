package loamstream.model.jobs

import loamstream.model.calls.{LKeys, LPileCall}
import loamstream.model.recipes.LPileCalls
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
case class LJob[Keys <: LKeys[_, _], Inputs <: LPileCalls[_, _],
Call <: LPileCall[Keys, Inputs]](call: Call, tool: LTool[Keys, Inputs, Call]) {

}
