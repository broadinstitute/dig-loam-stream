package loamstream.model.jobs

import loamstream.model.calls.LPileCall
import loamstream.model.tools.LTool

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
case class LJob[Call <: LPileCall](call: Call, tool: LTool[Call]) {

}
