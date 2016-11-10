package loamstream.model

import loamstream.util.TypeBox

/**
 * @author clint
 * @author Oliver
 * date: Apr 26, 2016
 */
trait Store extends LId.Owner {
  def sig: TypeBox.Untyped
}