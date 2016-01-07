package loamstream.model.calls

import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LMapTag

/**
  * LoamStream
  * Created by oliverr on 1/7/2016.
  */
case class LMapPreexisting[Tag <: LMapTag[_, _, _]](tag: Tag, id: String)
  extends LMapCall[Tag, LPileCalls.LCalls0] with LPilePreexisting[Tag] {

}
