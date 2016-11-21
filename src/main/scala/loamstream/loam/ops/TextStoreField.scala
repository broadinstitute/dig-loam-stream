package loamstream.loam.ops

import scala.util.Try

/** A field in a text file */
final case class TextStoreField[S <: TextStore, V](fieldTextExtractor: String => Option[String],
                                                   valueExtractor: String => Option[V])
  extends StoreField[S, V] {

  override def get(record: S#Record): Option[V] = fieldTextExtractor(record.text).flatMap(valueExtractor)
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
      if (columns.length <= iCol) {
        None
      } else {
        Some(columns(iCol))
      }
    }
  }

  object ValueExtractors {

    class ValueExtractorWrapper[V](val extractor: String => V) extends (String => Option[V]) {
      override def apply(string: String): Option[V] = Try(extractor(string)).toOption
    }

    def wrap[V](extractor: String => V): String => Option[V] = new ValueExtractorWrapper[V](extractor)

  }

  def columnField[S <: TextStore, V](sepRegEx: String, iCol: Int, valueExtractor: String => V): TextStoreField[S, V] =
    new TextStoreField[S, V](SeparatedColumnExtractor(sepRegEx, iCol), ValueExtractors.wrap(valueExtractor))

}

