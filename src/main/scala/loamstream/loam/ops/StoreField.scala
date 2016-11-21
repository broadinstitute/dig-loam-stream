package loamstream.loam.ops

/** Describes the field of a record of a Loam store */
trait StoreField[S <: StoreType, V] {

  /** Get Some(value) if defined, None else */
  def get(record: S#Record): Option[V]

  /** Get value if defined, default else */
  def getOrElse(record: S#Record, default: => V): V = get(record).getOrElse(default)

  /** True if value is defined */
  def isDefined(record: S#Record): Boolean = get(record).nonEmpty

}
