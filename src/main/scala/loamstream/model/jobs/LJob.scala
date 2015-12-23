package loamstream.model.jobs

import loamstream.model.calls.LPileCall
import loamstream.model.tags.LPileTag
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
case class LJob[Tag <: LPileTag[_, _], Call <: LPileCall[Tag]](call: Call, tool: LTool[Tag, Call]) {

}
