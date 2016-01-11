package loamstream.model.jobs

import loamstream.model.calls.LPileCall
import loamstream.model.calls.props.LProps
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LPileTag
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
case class LJob[Tag <: LPileTag[_, _], Inputs <: LPileCalls[_, _], +Props <: LProps,
Call <: LPileCall[Tag, Inputs, Props]](call: Call, tool: LTool[Tag, Inputs, Props, Call]) {

}
