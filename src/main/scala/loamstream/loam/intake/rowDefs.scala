package loamstream.loam.intake

/**
 * @author clint
 * Jan 17, 2020
 */
final case class RowDef(varIdDef: SourcedColumnDef, otherColumns: Seq[SourcedColumnDef]) {
  def columnDefs: Seq[SourcedColumnDef] = varIdDef +: otherColumns
}

object RowDef {
  def apply(varIdDef: UnsourcedColumnDef, otherColumns: Seq[UnsourcedColumnDef]): UnsourcedRowDef = {
    UnsourcedRowDef(varIdDef, otherColumns)
  }
}

final case class UnsourcedRowDef(varIdDef: UnsourcedColumnDef, otherColumns: Seq[UnsourcedColumnDef]) {
  def from(source: CsvSource): RowDef = RowDef(varIdDef.from(source), otherColumns.map(_.from(source)))
}
