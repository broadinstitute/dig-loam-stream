package loamstream.loam.ops

/**
  * LoamStream
  * Created by oliverr on 11/7/2016.
  */
final case class TextStoreField[T](fieldTextExtractor: String => Option[String],
                                   valueExtractor: String => Option[T])
  extends StoreField[T] {
  override type Record = TextStoreRecord

  override def get(record: Record): Option[T] = fieldTextExtractor(record.text).flatMap(valueExtractor)
}
