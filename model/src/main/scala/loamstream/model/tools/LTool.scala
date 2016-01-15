package loamstream.model.tools

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LSigTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LTool[+SigTag <: LSigTag, Inputs <: LPileCalls[_, _], Call <: LPileCall[SigTag, Inputs]] {
  def call: Call
}
