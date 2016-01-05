package loamstream.model.calls

import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LSetTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LSetCall[Tag <: LSetTag[_, _], Inputs <: LPileCalls[_, _, _]] extends LPileCall[Tag, Inputs] {

}
