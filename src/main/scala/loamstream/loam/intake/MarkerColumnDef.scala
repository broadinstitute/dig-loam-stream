package loamstream.loam.intake

final case class MarkerColumnDef(
    name: ColumnName,
    expr: ColumnExpr[Variant]) extends (DataRow => Variant) { self =>
  
  override def apply(row: DataRow): Variant = expr.apply(row)
}