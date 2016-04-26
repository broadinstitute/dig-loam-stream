package loamstream.model

import loamstream.model.piles.LPileSpec
import loamstream.model.id.LId

/**
 * @author clint
 * date: Apr 26, 2016
 */
trait StoreBase extends LId.Owner {
  def id: LId
  
  def spec: LPileSpec
  
  @deprecated
  final def pile: LPileSpec = spec
}