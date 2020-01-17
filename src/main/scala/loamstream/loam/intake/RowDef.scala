package loamstream.loam.intake

import org.apache.commons.csv.CSVRecord

final case class RowDef(varIdDef: SourcedColumnDef, otherColumns: Seq[SourcedColumnDef]) {
  def columnDefs: Seq[SourcedColumnDef] = varIdDef +: otherColumns
}
