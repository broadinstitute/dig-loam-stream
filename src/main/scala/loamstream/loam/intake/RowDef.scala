package loamstream.loam.intake


final case class RowDef(varIdDef: SourcedColumnDef, otherColumns: Seq[SourcedColumnDef]) {
  def columnDefs: Seq[SourcedColumnDef] = varIdDef +: otherColumns
}
