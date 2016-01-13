package loamstream.model.tools

import loamstream.model.calls.LPileCall
import loamstream.model.calls.props.LProps
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LTags
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait ToolMapper {
  def findTool[Tags <: LTags, Inputs <: LPileCalls[_, _], Props <: LProps,
  Call <: LPileCall[Tags, Inputs, Props]]: Shot[LTool[Tags, Inputs, Props, Call]]
}
