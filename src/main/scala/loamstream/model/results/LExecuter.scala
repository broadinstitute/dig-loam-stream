package loamstream.model.results

import loamstream.model.calls.LPileCall
import loamstream.model.tags.LPileTag
import loamstream.model.tools.LTool
import util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LExecuter {

  def execute[Tag <: LPileTag[_, _], Call <: LPileCall[Tag], Tool <: LTool[Tag, Call]]: Shot[LResult[Tag, Call, Tool]]

}
