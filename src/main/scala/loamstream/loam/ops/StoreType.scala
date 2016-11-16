package loamstream.loam.ops

/** Type of a LoamStore */
trait StoreType {
  type Record <: StoreRecord
}
