package loamstream.model.stores

import loamstream.model.id.LId
import loamstream.model.piles.LPileSpec

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
trait LStore extends LId.Owner {
  def pile: LPileSpec
}
