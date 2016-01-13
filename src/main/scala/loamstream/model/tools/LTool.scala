package loamstream.model.tools

import loamstream.model.calls.LPileCall
import loamstream.model.calls.props.LProps
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LTags

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LTool[+Tags <: LTags, Inputs <: LPileCalls[_, _], +Props <: LProps,
Call <: LPileCall[Tags, Inputs, Props]] {
  def call: Call
}
