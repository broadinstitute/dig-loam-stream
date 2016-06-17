package loamstream.model

/**
 * @author clint
 * @author Oliver
 * date: Apr 26, 2016
 */
trait Store extends LId.Owner {
  def sig: StoreSig

  final def toTuple: (LId, StoreSig) = (id, sig)
}