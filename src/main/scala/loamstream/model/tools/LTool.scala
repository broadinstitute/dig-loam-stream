package loamstream.model.tools

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LPileTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LTool[Tag <: LPileTag[_, _], Inputs <: LPileCalls[_, _, _], Call <: LPileCall[Tag, Inputs]] {
  def tag: Tag
  def call: Call
}
