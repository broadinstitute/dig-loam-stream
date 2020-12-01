package loamstream.loam.intake

import org.apache.commons.csv.CSVRecord
import loamstream.loam.intake.flip.Disposition
import scala.util.Failure

/**
 * @author clint
 * Feb 10, 2020
 */
trait CsvRow extends SkippableRow[CsvRow] with KeyedRow with IndexedRow {
  def recordNumber: Long
}

object CsvRow {
  final case class CommonsCsvRow(delegate: CSVRecord, isSkipped: Boolean = false) extends CsvRow {
    override def getFieldByName(name: String): String = delegate.get(name)
    
    override def getFieldByIndex(i: Int): String = delegate.get(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.getRecordNumber
    
    override def skip: CommonsCsvRow = copy(isSkipped = true)
  }
}
