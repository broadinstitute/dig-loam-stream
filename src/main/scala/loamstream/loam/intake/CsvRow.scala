package loamstream.loam.intake

import org.apache.commons.csv.CSVRecord
import loamstream.loam.intake.flip.Disposition

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
  abstract class WithFlipTag(
      val marker: String, 
      val originalMarker: String, 
      val disposition: Disposition) extends CsvRow {
    
    final def isFlipped: Boolean = disposition.isFlipped
    final def notFlipped: Boolean = disposition.notFlipped
    
    final def isSameStrand: Boolean = disposition.isSameStrand
    final def isComplementStrand: Boolean = disposition.isComplementStrand
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
      override val originalMarker: String,
      override val disposition: Disposition) extends CsvRow.WithFlipTag(marker, originalMarker, disposition) {
    
    override def getFieldByName(name: String): String = delegate.getFieldByName(name)
    
    override def getFieldByIndex(i: Int): String = delegate.getFieldByIndex(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.recordNumber
  }
}
