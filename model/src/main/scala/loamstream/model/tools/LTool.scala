package loamstream.model.tools

import loamstream.model.calls.{LKeys, LPileCall}
import loamstream.model.recipes.LPileCalls

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LTool[Keys <: LKeys[_, _], Inputs <: LPileCalls[_, _],
Call <: LPileCall[Keys, Inputs]] {
  def call: Call
}
