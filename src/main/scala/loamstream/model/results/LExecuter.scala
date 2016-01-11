package loamstream.model.results

import loamstream.model.calls.LPileCall
import loamstream.model.calls.props.LProps
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LPileTag
import loamstream.model.tools.LTool
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LExecuter {

  def execute[Tag <: LPileTag[_, _], Inputs <: LPileCalls[_, _], Props <: LProps,
  Call <: LPileCall[Tag, Inputs, Props], Tool <: LTool[Tag, Inputs, Props, Call]]:
  Shot[LResult[Tag, Inputs, Props, Call, Tool]]

}
