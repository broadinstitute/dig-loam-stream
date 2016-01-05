package loamstream.model.calls

import loamstream.model.recipes.LPileCalls
import loamstream.model.tags.LMapTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LMapCall[Tag <: LMapTag[_, _, _], Inputs <: LPileCalls[_, _, _]] extends LPileCall[Tag, Inputs] {

}
