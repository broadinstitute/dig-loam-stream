package loamstream.loam.intake

import scala.util.Try
import scala.util.Failure
import scala.util.Success

/**
 * @author clint
 * Oct 14, 2020
 */
final case class AggregatorRowExpr(
    metadata: AggregatorMetadata,
    markerDef: MarkerColumnDef,
    pvalueDef: NamedColumnDef[Double],
    zscoreDef: Option[NamedColumnDef[Double]] = None,
    stderrDef: Option[NamedColumnDef[Double]] = None,
    betaDef: Option[NamedColumnDef[Double]] = None,
    oddsRatioDef: Option[NamedColumnDef[Double]] = None,
    eafDef: Option[NamedColumnDef[Double]] = None,
    mafDef: Option[NamedColumnDef[Double]] = None,
    nDef: Option[NamedColumnDef[Double]] = None,
    alleleCountDef: Option[NamedColumnDef[Long]] = None,
    alleleCountCasesDef: Option[NamedColumnDef[Long]] = None,
    alleleCountControlsDef: Option[NamedColumnDef[Long]] = None,
    heterozygousCasesDef: Option[NamedColumnDef[Long]] = None, 
    heterozygousControlsDef: Option[NamedColumnDef[Long]] = None, 
    homozygousCasesDef: Option[NamedColumnDef[Long]] = None, 
    homozygousControlsDef: Option[NamedColumnDef[Long]] = None, 
    derivedFromRecordNumberDef: Option[NamedColumnDef[Long]] = None,
    failFast: Boolean = false) extends TaggedRowParser[VariantRow.Parsed] {
  
  private object metadataColumnDefs {
    val dataset = LiteralColumnExpr(metadata.dataset)
    val phenotype = LiteralColumnExpr(metadata.phenotype)
    val ancestry = LiteralColumnExpr(metadata.ancestry)
  }
  
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
      nDef ++
      alleleCountDef ++
      alleleCountCasesDef ++
      alleleCountControlsDef ++
      heterozygousCasesDef ++ 
      heterozygousControlsDef ++ 
      homozygousCasesDef ++ 
      homozygousControlsDef).map(_.name).toSeq
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
  
  override def apply(row: VariantRow.Tagged): VariantRow.Parsed = { 
    def transformed = VariantRow.Transformed(
      derivedFrom = row, 
      aggRow = AggregatorVariantRow(
        marker = row.marker,
        pvalue = pvalueDef.apply(row),
        dataset = metadataColumnDefs.dataset(row),
        phenotype = metadataColumnDefs.phenotype(row),
        ancestry = metadataColumnDefs.ancestry(row),
        zscore = zscoreDef.map(_.apply(row)),
        stderr = stderrDef.map(_.apply(row)),
        beta = betaDef.map(_.apply(row)),
        oddsRatio = oddsRatioDef.map(_.apply(row)),
        eaf = eafDef.map(_.apply(row)),
        maf = mafDef.map(_.apply(row)),
        n = nDef.map(_.apply(row)),
        alleleCount = alleleCountDef.map(_.apply(row)),
        alleleCountCases = alleleCountCasesDef.map(_.apply(row)), 
        alleleCountControls = alleleCountControlsDef.map(_.apply(row)),
        heterozygousCases = heterozygousCasesDef.map(_.apply(row)), 
        heterozygousControls = heterozygousControlsDef.map(_.apply(row)), 
        homozygousCases = homozygousCasesDef.map(_.apply(row)), 
        homozygousControls = homozygousControlsDef.map(_.apply(row)),
        derivedFromRecordNumber = Some(row.recordNumber)))
        
    def skipped(cause: Option[Failure[VariantRow.Parsed]]) = VariantRow.Skipped(row, aggRowOpt = None, cause = cause)
        
    if(row.isSkipped) {
      skipped(cause = None)
    } else {
      val attempt: Try[VariantRow.Parsed] = Try(transformed)
      
      attempt match {
        case Success(r) => r
        case f @ Failure(e) => if(failFast) attempt.get else attempt.getOrElse(skipped(Some(f)))
      }
    }
  }
}

