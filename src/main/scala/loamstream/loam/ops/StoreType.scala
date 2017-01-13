package loamstream.loam.ops

import TextStoreField.{columnField, ColumnSeparators}

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

  trait BIM extends TextStore

  // scalastyle:off magic.number
  object BIM {
    import ColumnSeparators.blankOrTab
    
    val chr = columnField[BIM, Int](blankOrTab, 0, _.toInt)
    val id = columnField[BIM, String](blankOrTab, 1, identity)
    val posPhysical = columnField[BIM, Double](blankOrTab, 2, _.toDouble)
    val pos = columnField[BIM, Int](blankOrTab, 3, _.toInt)
    val allele1 = columnField[BIM, String](blankOrTab, 4, identity)
    val allele2 = columnField[BIM, String](blankOrTab, 5, identity)
  }
  // scalastyle:on magic.number

}