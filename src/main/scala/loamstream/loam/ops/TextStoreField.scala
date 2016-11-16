package loamstream.loam.ops

import scala.util.Try

/** A field in a text file */
final case class TextStoreField[Value](fieldTextExtractor: String => Option[String],
                                       valueExtractor: String => Option[Value])
  extends StoreField[Value] {
  override type Record = TextStoreRecord

  override def get(record: Record): Option[Value] = fieldTextExtractor(record.text).flatMap(valueExtractor)
}

/** A field in a text file */
object TextStoreField {

  object ColumnSeparators {
    val blank = " "
    val tab = "\t"
    val blankOrTab = "[\\t ]"
  }

  case class SeparatedColumnExtractor(sepRegEx: String, iCol: Int) extends (String => Option[String]) {
    override def apply(line: String): Option[String] = {
      val columns = line.split(sepRegEx)
      if (columns.length < iCol) {
        None
      } else {
        Some(columns(iCol))
      }
    }
  }

  object ValueExtractors {

    class ValueExtractorWrapper[Value](val extractor: String => Value) extends (String => Option[Value]) {
      override def apply(string: String): Option[Value] = Try(extractor(string)).toOption
    }

    def wrap[Value](extractor: String => Value): String => Option[Value] = new ValueExtractorWrapper[Value](extractor)

  }

  def columnField[Value](sepRegEx: String, iCol: Int, valueExtractor: String => Value): TextStoreField[Value] =
    new TextStoreField[Value](SeparatedColumnExtractor(sepRegEx, iCol), ValueExtractors.wrap(valueExtractor))

}

