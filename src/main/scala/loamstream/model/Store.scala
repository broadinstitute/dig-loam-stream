package loamstream.model

import loamstream.model.piles.LPileSpec

/**
 * @author clint
 * @author Oliver
 * date: Apr 26, 2016
 */
trait Store extends LId.Owner {
  def spec: LPileSpec
}