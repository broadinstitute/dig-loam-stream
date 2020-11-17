package loamstream.loam.intake

/**
 * @author clint
 * Nov 16, 2020
 */
final case class RowTuple(rawRow: CsvRow.TaggedCsvRow, dataRow: DataRow) {
  def skip: RowTuple = copy(rawRow = rawRow.skip)
}
