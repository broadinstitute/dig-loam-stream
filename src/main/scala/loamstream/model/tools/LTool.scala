package loamstream.model.tools

import loamstream.model.calls.LPileCall
import loamstream.model.tags.LPileTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LTool[Tag <: LPileTag[_, _], Call <: LPileCall[Tag]] {
  def tag: Tag
  def call: Call
}
