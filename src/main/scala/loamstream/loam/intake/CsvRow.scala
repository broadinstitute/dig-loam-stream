package loamstream.loam.intake

/**
 * @author clint
 * Feb 10, 2020
 */
trait CsvRow {
  def getFieldByName(name: String): String
  
  def getFieldByIndex(i: Int): String
}

object CsvRow {
  final case class FastCsvRow(delegate: de.siegmar.fastcsv.reader.CsvRow) extends CsvRow {
    override def getFieldByName(name: String): String = delegate.getField(name)
    
    override def getFieldByIndex(i: Int): String = delegate.getField(i)
  }
}
