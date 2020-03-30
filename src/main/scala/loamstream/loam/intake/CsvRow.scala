package loamstream.loam.intake

import org.apache.commons.csv.CSVRecord

/**
 * @author clint
 * Feb 10, 2020
 */
trait CsvRow {
  def getFieldByName(name: String): String
  
  def getFieldByIndex(i: Int): String
}

object CsvRow {
  final case class CommonsCsvRow(delegate: CSVRecord) extends CsvRow {
    override def getFieldByName(name: String): String = delegate.get(name)
    
    override def getFieldByIndex(i: Int): String = delegate.get(i)
  }
}
