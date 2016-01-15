package loamstream.model.results

import loamstream.model.calls.{LKeys, LPileCall}
import loamstream.model.recipes.LPileCalls
import loamstream.model.tools.LTool
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LExecuter {

  def execute[Keys <: LKeys[_, _], Inputs <: LPileCalls[_, _], Call <: LPileCall[Keys, Inputs],
  Tool <: LTool[Keys, Inputs, Call]]: Shot[LResult[Keys, Inputs, Call, Tool]]

}
