package loamstream.loam.intake.aggregator

import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.CsvRow
import loamstream.loam.intake.ColumnDef
import loamstream.loam.intake.NamedColumnDef

/**
 * @author clint
 * Oct 14, 2020
 */
final case class RowExpr(
    markerDef: NamedColumnDef[String],
    pvalueDef: NamedColumnDef[Double],
    zscoreDef: Option[NamedColumnDef[Double]] = None,
    stderrDef: Option[NamedColumnDef[Double]] = None,
    betaDef: Option[NamedColumnDef[Double]] = None,
    oddsRatioDef: Option[NamedColumnDef[Double]] = None,
    eafDef: Option[NamedColumnDef[Double]] = None,
    mafDef: Option[NamedColumnDef[Double]] = None,
    nDef: Option[NamedColumnDef[Double]] = None) extends TaggedRowParser[DataRow] {
  
  def columnDefs: Seq[NamedColumnDef[_]] = {
    //NB: Note that this order matters. :\ 
    markerDef +: 
    pvalueDef +: {
      (zscoreDef ++
      stderrDef ++
      betaDef ++
      oddsRatioDef ++
      eafDef ++
      mafDef ++
      nDef).toSeq
    }
  }
  
  def sourceColumns: SourceColumns = {
    def nameOf(columnDef: NamedColumnDef[_]) = columnDef.name.mapName(_.toLowerCase)
    
    SourceColumns(
      marker = nameOf(markerDef), 
      pvalue = nameOf(pvalueDef),
      zscore = zscoreDef.map(nameOf),
      stderr = stderrDef.map(nameOf),
      beta = betaDef.map(nameOf),
      oddsRatio = oddsRatioDef.map(nameOf),
      eaf = eafDef.map(nameOf),
      maf = mafDef.map(nameOf),
      n = nDef.map(nameOf))
  }
  
  override def apply(row: CsvRow.WithFlipTag): DataRow = DataRow(
    marker = if(row.notFlipped) row.marker else markerDef.apply(row),
    pvalue = pvalueDef.apply(row),
    zscore = zscoreDef.map(_.apply(row)),
    stderr = stderrDef.map(_.apply(row)),
    beta = betaDef.map(_.apply(row)),
    oddsRatio = oddsRatioDef.map(_.apply(row)),
    eaf = eafDef.map(_.apply(row)),
    maf = mafDef.map(_.apply(row)),
    n = nDef.map(_.apply(row)))
}

