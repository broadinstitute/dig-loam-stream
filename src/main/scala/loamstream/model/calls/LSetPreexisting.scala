package loamstream.model.calls

import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LSetTag

/**
  * LoamStream
  * Created by oliverr on 1/7/2016.
  */
case class LSetPreexisting[Tag <: LSetTag[_, _]](tag: Tag, id: String)
  extends LSetCall[Tag, LPileCalls.LCalls0] with LPilePreexisting[Tag] {

}
