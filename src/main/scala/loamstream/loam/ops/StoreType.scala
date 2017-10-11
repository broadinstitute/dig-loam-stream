package loamstream.loam.ops

/** Type of a LoamStore */
trait StoreType {
  /** The type of records in the store */
  type Record <: StoreRecord
}

object StoreType {
  trait VCF extends TextStore

  trait TXT extends TextStore {
    override type Record = TextStoreRecord
  }
}