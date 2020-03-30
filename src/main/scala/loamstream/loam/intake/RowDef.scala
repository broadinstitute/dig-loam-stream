package loamstream.loam.intake

/**
 * @author clint
 * Jan 17, 2020
 */
final case class RowDef(varIdDef: SourcedColumnDef, otherColumns: Seq[SourcedColumnDef]) {
  def columnDefs: Seq[SourcedColumnDef] = varIdDef +: otherColumns
}
