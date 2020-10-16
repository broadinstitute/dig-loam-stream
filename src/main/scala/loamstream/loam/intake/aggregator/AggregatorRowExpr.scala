package loamstream.loam.intake.aggregator

import loamstream.loam.intake.RowExpr
import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.CsvRow
import loamstream.loam.intake.RowDef
import loamstream.loam.intake.ColumnDef
import loamstream.loam.intake.NamedColumnDef

/**
 * @author clint
 * Oct 14, 2020
 */
final case class AggregatorRowExpr(
    markerDef: NamedColumnDef[String],
    pvalueDef: NamedColumnDef[Double],
    zscoreDef: Option[NamedColumnDef[Double]] = None,
    stderrDef: Option[NamedColumnDef[Double]] = None,
    betaDef: Option[NamedColumnDef[Double]] = None,
    oddsRatioDef: Option[NamedColumnDef[Double]] = None,
    eafDef: Option[NamedColumnDef[Double]] = None,
    mafDef: Option[NamedColumnDef[Double]] = None,
    nDef: Option[NamedColumnDef[Double]] = None) extends RowExpr[DataRow] {
  
  def columnDefs: Seq[NamedColumnDef[_]] = {
    markerDef +: 
    pvalueDef +: {
      (zscoreDef ++
      stderrDef ++
      betaDef ++
      oddsRatioDef ++
      eafDef ++
      mafDef ++
      nDef).toSeq.sortBy(_.name.index)
    }
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

object AggregatorRowExpr {
  /*def fromRowDef(rowDef: RowDef): AggregatorRowExpr = AggregatorRowExpr(
      markerExpr
      )
  }*/
}
