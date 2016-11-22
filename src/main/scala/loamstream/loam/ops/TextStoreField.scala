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
    //NB: These are all regexes, but since they're passed to Java APIs as strings, 
    //we don't make them into Regex objects
    val blank: String = " "
    val tab: String = "\t"
    val blankOrTab: String = "[\\t ]"
  }

  final case class SeparatedColumnExtractor(sepRegEx: String, columnIndex: Int) extends (String => Option[String]) {
    override def apply(line: String): Option[String] = {
      val columns = line.split(sepRegEx)
      if (columns.length <= columnIndex) {
        None
      } else {
        Some(columns(columnIndex))
      }
    }
  }

  object ValueExtractors {

    final class ValueExtractorWrapper[V](val extractor: String => V) extends (String => Option[V]) {
      override def apply(string: String): Option[V] = Try(extractor(string)).toOption
    }

    def wrap[V](extractor: String => V): String => Option[V] = new ValueExtractorWrapper[V](extractor)

  }

  def columnField[S <: TextStore, V](
      sepRegEx: String, 
      columnIndex: Int, 
      valueExtractor: String => V): TextStoreField[S, V] = {
    
    TextStoreField[S, V](SeparatedColumnExtractor(sepRegEx, columnIndex), ValueExtractors.wrap(valueExtractor))
  }

}

