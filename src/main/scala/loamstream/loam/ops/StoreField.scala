package loamstream.loam.ops

/** Describes the field of a record of a Loam store */
trait StoreField[Store <: StoreType, Value] {

  /** Get Some(value) if defined, None else */
  def get(record: Store#Record): Option[Value]

  /** Get value if defined, default else */
  def getOrElse(record: Store#Record, default: => Value): Value = get(record).getOrElse(default)

  /** True if value si defined */
  def isDefined(record: Store#Record): Boolean = get(record).nonEmpty

}
