package loamstream.model.calls

import loamstream.model.calls.props.LProps
import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LMapTag

/**
  * LoamStream
  * Created by oliverr on 1/7/2016.
  */
case class LMapPreexisting[Tag <: LMapTag[_, _, _], +Props <: LProps](tag: Tag, id: String)
  extends LMapCall[Tag, LPileCalls.LCalls0, Props] with LPilePreexisting[Tag, Props] {

}
