package loamstream.model.results

import loamstream.model.calls.LPileCall
import loamstream.model.calls.props.LProps
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LTags
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
class LResult[+Tags <: LTags, Inputs <: LPileCalls[_, _], Props <: LProps,
Call <: LPileCall[Tags, Inputs, Props], Tool <: LTool[Tags, Inputs, Props, Call]] {

}
