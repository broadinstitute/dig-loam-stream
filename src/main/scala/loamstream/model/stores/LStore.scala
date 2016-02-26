package loamstream.model.stores

import loamstream.model.piles.{LPileSpec, LPile}

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
trait LStore {
  def pile: LPile
  def pileSpec: LPileSpec = pile.spec
}
