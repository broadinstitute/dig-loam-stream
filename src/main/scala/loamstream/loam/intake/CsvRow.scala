package loamstream.loam.intake

import org.apache.commons.csv.CSVRecord

/**
 * @author clint
 * Feb 10, 2020
 */
trait CsvRow {
  def getFieldByName(name: String): String
  
  def getFieldByIndex(i: Int): String
  
  def size: Int
  
  def values: Iterator[String] = (0 until size).iterator.map(getFieldByIndex)
  
  def recordNumber: Long
}

object CsvRow {
  abstract class WithFlipTag(val marker: String, val isFlipped: Boolean) extends CsvRow {
    final def notFlipped: Boolean = !isFlipped
  }
  
  final case class CommonsCsvRow(delegate: CSVRecord) extends CsvRow {
    override def getFieldByName(name: String): String = delegate.get(name)
    
    override def getFieldByIndex(i: Int): String = delegate.get(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.getRecordNumber
  }
  
  final case class TaggedCsvRow(
      delegate: CsvRow,
      override val marker: String,
      override val isFlipped: Boolean) extends CsvRow.WithFlipTag(marker, isFlipped) {
    
    override def getFieldByName(name: String): String = delegate.getFieldByName(name)
    
    override def getFieldByIndex(i: Int): String = delegate.getFieldByIndex(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.recordNumber
  }
}
