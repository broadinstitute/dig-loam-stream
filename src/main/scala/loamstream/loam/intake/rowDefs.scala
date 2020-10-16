package loamstream.loam.intake

/**
 * @author clint
 * Jan 17, 2020
 */
final case class RowDef(
    varIdDef: NamedColumnDef[String], 
    otherColumns: Seq[NamedColumnDef[_]]) extends RowExpr[CsvRow] {
  
  def columnDefs: Seq[NamedColumnDef[_]] = varIdDef +: otherColumns
  
  override def apply(input: CsvRow.WithFlipTag): CsvRow = apply(input)
}
