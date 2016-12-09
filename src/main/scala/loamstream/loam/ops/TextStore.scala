package loamstream.loam.ops

/** A store whose records are Strings */
trait TextStore extends StoreType {
  override type Record <: TextStoreRecord
}
