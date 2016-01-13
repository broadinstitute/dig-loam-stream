package loamstream.model.jobs

import loamstream.model.calls.LPileCall
import loamstream.model.calls.props.LProps
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LTags
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
case class LJob[+Tags <: LTags, Inputs <: LPileCalls[_, _], +Props <: LProps,
Call <: LPileCall[Tags, Inputs, Props]](call: Call, tool: LTool[Tags, Inputs, Props, Call]) {

}
