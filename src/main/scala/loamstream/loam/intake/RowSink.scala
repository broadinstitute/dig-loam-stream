package loamstream.loam.intake

/**
 * @author clint
 * Oct 13, 2020
 */
trait RowSink {
  def accept(row: CsvRow): Unit
}
