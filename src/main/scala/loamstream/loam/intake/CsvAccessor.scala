package loamstream.loam.intake

/**
 * @author clint
 * Jan 17, 2020
 */
trait CsvAccessor[A] extends (String => A => String)

object CsvAccessor {
  object FastCsv extends CsvAccessor[de.siegmar.fastcsv.reader.CsvRow] {
    override def apply(columnName: String): de.siegmar.fastcsv.reader.CsvRow => String = _.getField(columnName)
  }
}
