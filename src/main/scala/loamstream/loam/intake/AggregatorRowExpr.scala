package loamstream.loam.intake

import scala.util.Try
import scala.util.Failure
import scala.util.Success

/**
 * @author clint
 * Oct 14, 2020
 */
final case class AggregatorRowExpr(
    markerDef: MarkerColumnDef,
    pvalueDef: NamedColumnDef[Double],
    zscoreDef: Option[NamedColumnDef[Double]] = None,
    stderrDef: Option[NamedColumnDef[Double]] = None,
    betaDef: Option[NamedColumnDef[Double]] = None,
    oddsRatioDef: Option[NamedColumnDef[Double]] = None,
    eafDef: Option[NamedColumnDef[Double]] = None,
    mafDef: Option[NamedColumnDef[Double]] = None,
    nDef: Option[NamedColumnDef[Double]] = None,
    failFast: Boolean = false) extends TaggedRowParser[CsvRow.Parsed] {
  
  def columnNames: Seq[ColumnName] = {
    //NB: Note that this order matters. :\ 
    markerDef.name +: 
    pvalueDef.name +: {
      (zscoreDef ++
      stderrDef ++
      betaDef ++
      oddsRatioDef ++
      eafDef ++
      mafDef ++
      nDef).map(_.name).toSeq
    }
  }
  
  def sourceColumns: SourceColumns = {
    def nameOfMarker(columnDef: MarkerColumnDef) = columnDef.name.mapName(_.toLowerCase)
    def nameOf(columnDef: NamedColumnDef[_]) = columnDef.name.mapName(_.toLowerCase)
    
    SourceColumns(
      marker = nameOfMarker(markerDef), 
      pvalue = nameOf(pvalueDef),
      zscore = zscoreDef.map(nameOf),
      stderr = stderrDef.map(nameOf),
      beta = betaDef.map(nameOf),
      oddsRatio = oddsRatioDef.map(nameOf),
      eaf = eafDef.map(nameOf),
      maf = mafDef.map(nameOf),
      n = nDef.map(nameOf))
  }
  
  override def apply(row: CsvRow.Tagged): CsvRow.Parsed = { 
    def transformed = CsvRow.Transformed(
      derivedFrom = row, 
      dataRow = DataRow(
        marker = row.marker,
        pvalue = pvalueDef.apply(row),
        zscore = zscoreDef.map(_.apply(row)),
        stderr = stderrDef.map(_.apply(row)),
        beta = betaDef.map(_.apply(row)),
        oddsRatio = oddsRatioDef.map(_.apply(row)),
        eaf = eafDef.map(_.apply(row)),
        maf = mafDef.map(_.apply(row)),
        n = nDef.map(_.apply(row))))
        
    def skipped(cause: Option[Failure[CsvRow.Parsed]]) = CsvRow.Skipped(row, dataRowOpt = None, cause = cause)
        
    if(row.isSkipped) {
      skipped(cause = None)
    } else {
      val attempt: Try[CsvRow.Parsed] = Try(transformed)
      
      attempt match {
        case Success(r) => r
        case f @ Failure(_) => if(failFast) attempt.get else attempt.getOrElse(skipped(Some(f)))
      }
    }
  }
}

