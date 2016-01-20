package loamstream.model.results

import loamstream.model.calls.LPileCall
import loamstream.model.tools.LTool
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LExecuter {

  def execute[Call <: LPileCall, Tool <: LTool[Call]]: Shot[LResult[Call, Tool]]

}
