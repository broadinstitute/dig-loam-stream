package loamstream.loam.ops

/** Type of a LoamStore */
trait StoreType {
  type Record <: StoreRecord
}

object StoreType {
  trait VCF extends TextStore

  trait TXT extends TextStore {
    override type Record = TextStoreRecord
  }

  trait BIM extends TextStore

  object BIM {
    val chr = TextStoreField.columnField[BIM, Int](TextStoreField.ColumnSeparators.blankOrTab, 0, _.toInt)
  }

}