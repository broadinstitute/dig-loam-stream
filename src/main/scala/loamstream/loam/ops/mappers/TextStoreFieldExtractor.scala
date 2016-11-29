package loamstream.loam.ops.mappers

import loamstream.loam.ops.StoreType.TXT
import loamstream.loam.ops.{StoreRecord, TextStore, TextStoreField, TextStoreRecord}

import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}


/** Extracting a given field */
case class TextStoreFieldExtractor[SI <: TextStore : TypeTag, V](field: TextStoreField[SI, V], defaultString: String)
  extends LoamStoreMapper[SI, TXT] {

  val tpeIn: Type = typeOf[SI]
  val tpeOut: Type = typeOf[TXT]

  override def mapDynamicallyTyped(record: StoreRecord, tpeIn: Type, tpeOut: Type): StoreRecord =
    if (record.isInstanceOf[TextStoreRecord] && tpeIn <:< this.tpeIn && this.tpeOut <:< tpeOut) {
      map(record.asInstanceOf[SI#Record])
    } else {
      TextStoreRecord(defaultString)
    }


  /** Map a record */
  override def map(record: SI#Record): TextStoreRecord =
  TextStoreRecord(field.fieldTextExtractor(record.text).getOrElse(defaultString))
}
