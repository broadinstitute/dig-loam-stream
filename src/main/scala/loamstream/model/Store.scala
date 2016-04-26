package loamstream.model

import loamstream.model.piles.LPileSpec
import loamstream.model.id.LId

/**
 * @author clint
 * date: Apr 26, 2016
 */
trait Store extends LId.Owner {
  def spec: LPileSpec
}