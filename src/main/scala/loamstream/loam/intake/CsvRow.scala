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
  final case class CommonsCsvRow(delegate: CSVRecord) extends CsvRow {
    override def getFieldByName(name: String): String = delegate.get(name)
    
    override def getFieldByIndex(i: Int): String = delegate.get(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.getRecordNumber
  }
  
  final case class Tagged(
      delegate: CsvRow,
      marker: Variant,
      originalMarker: Variant,
      disposition: Disposition,
      skipped: Boolean = false) extends CsvRow {
    
    def isFlipped: Boolean = disposition.isFlipped
    def notFlipped: Boolean = disposition.notFlipped
    
    def isSameStrand: Boolean = disposition.isSameStrand
    def isComplementStrand: Boolean = disposition.isComplementStrand
    
    override def getFieldByName(name: String): String = delegate.getFieldByName(name)
    
    override def getFieldByIndex(i: Int): String = delegate.getFieldByIndex(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.recordNumber
    
    def skip: Tagged = copy(skipped = true)
  }
  
  sealed trait Parsed extends CsvRow {
    val derivedFrom: Tagged
    
    def dataRowOpt: Option[DataRow]
    
    def skipped: Boolean
    
    def skip: Parsed
    
    def transform(f: DataRow => DataRow): Parsed
    
    final def isFlipped: Boolean = derivedFrom.isFlipped
    final def notFlipped: Boolean = derivedFrom.notFlipped
    
    final def isSameStrand: Boolean = derivedFrom.isSameStrand
    final def isComplementStrand: Boolean = derivedFrom.isComplementStrand
    
    override def getFieldByName(name: String): String = derivedFrom.getFieldByName(name)
    
    override def getFieldByIndex(i: Int): String = derivedFrom.getFieldByIndex(i)
    
    override def size: Int = derivedFrom.size
    
    override def recordNumber: Long = derivedFrom.recordNumber
  }
  
  final case class Transformed(
      derivedFrom: Tagged,
      dataRow: DataRow) extends Parsed {
    
    override def dataRowOpt: Option[DataRow] = Some(dataRow)
    
    override def skipped: Boolean = false
    
    override def skip: Skipped = Skipped(derivedFrom, dataRowOpt)
    
    override def transform(f: DataRow => DataRow): Transformed = copy(dataRow = f(dataRow))
  }
  
  final case class Skipped(derivedFrom: Tagged, dataRowOpt: Option[DataRow]) extends Parsed {
    override def skipped: Boolean = true
    
    override def skip: Skipped = this
    
    override def transform(f: DataRow => DataRow): Skipped = this
  }
}
