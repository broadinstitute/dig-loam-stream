package loamstream.model.tools

import loamstream.model.calls.LPileCall

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LTool[Call <: LPileCall] {
  def call: Call
}
