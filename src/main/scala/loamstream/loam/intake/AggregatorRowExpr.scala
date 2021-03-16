package loamstream.loam.intake

import scala.util.Try
import scala.util.Failure
import scala.util.Success
import loamstream.loam.intake.metrics.BioIndexClient

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
    casesDef: Option[NamedColumnDef[Double]] = None,
    controlsDef: Option[NamedColumnDef[Double]] = None,
    subjectsDef: Option[NamedColumnDef[Double]] = None,
    nDef: Option[NamedColumnDef[Double]] = None,
    alleleCountDef: Option[NamedColumnDef[Long]] = None,
    alleleCountCasesDef: Option[NamedColumnDef[Long]] = None,
    alleleCountControlsDef: Option[NamedColumnDef[Long]] = None,
    heterozygousCasesDef: Option[NamedColumnDef[Long]] = None, 
    heterozygousControlsDef: Option[NamedColumnDef[Long]] = None, 
    homozygousCasesDef: Option[NamedColumnDef[Long]] = None, 
    homozygousControlsDef: Option[NamedColumnDef[Long]] = None, 
    failFast: Boolean = false,
    private val bioIndexClient: BioIndexClient = new BioIndexClient.Default()) extends 
        TaggedRowParser[VariantRow.Parsed] {
  
  private object metadataColumnDefs {
    val dataset = LiteralColumnExpr(metadata.dataset)
    val phenotype = LiteralColumnExpr(metadata.phenotype)
    val ancestry = LiteralColumnExpr(metadata.ancestry)
  }
  
  private object Inferred {
    //NB: calculate missing values where possible
    lazy val beta: Option[NamedColumnDef[Double]] = (betaDef, oddsRatioDef) match {
      //TODO: logarithm base?
      case (None, Some(orDef)) => Some(orDef.map(scala.math.log)) 
      case _ => None
    }

    lazy val oddsRatio: Option[NamedColumnDef[Double]] = (oddsRatioDef, betaDef) match {
      case (None, Some(bDef)) => Some(bDef.map(scala.math.exp))
      case _ => None
    }

    private def qnorm(beta: NamedColumnDef[Double], pvalue: NamedColumnDef[Double]): NamedColumnDef[Double] = {
      ???
    }
    
    lazy val stderr: Option[NamedColumnDef[Double]] = (stderrDef, betaDef) match {
      case (None, Some(bDef)) => Some(qnorm(bDef, pvalueDef))
      case _ => None
    }
    
    lazy val zscore: Option[NamedColumnDef[Double]] = (zscoreDef, betaDef, stderr) match {
      case (None, Some(bDef), Some(seDef)) => Some(bDef / seDef)
      case _ => None
    }
    
    lazy val maf: Option[NamedColumnDef[Double]] = (mafDef, eafDef) match {
      case (None, Some(eDef)) => Some(eDef.map(e => if(e < 0.5) e else 1.0 - e))
      case _ => None
    }
    
    private def effectiveN: NamedColumnDef[Double] = {
      val canonicalPhenotypeOpt = bioIndexClient.findClosestMatch(Phenotype(metadata.phenotype, false))
      
      require(
          canonicalPhenotypeOpt.isDefined, 
          s"Could not find canonical name and dichotomous status for phenotype '${metadata.phenotype}'")
          
      val canonicalPhenotype = canonicalPhenotypeOpt.get
      
      //if the phenotype is dichotomous, use subjects, else cases+controls
      (canonicalPhenotype.dichotomous, casesDef, controlsDef, subjectsDef) match {
        case (true, Some(cases), Some(controls), _) => cases.combine(name = ???)(controls) { 
          Stats.effectiveN(_, _)
        }
        case (true, None, _, _) => sys.error(s"Cases expression missing for dichotomous phenotype '${metadata.phenotype}'")
        case (true, _, None, _) => sys.error(s"Controls expression missing for dichotomous phenotype '${metadata.phenotype}'")
        case (_, _, _, Some(subjects)) => subjects
        case _ => sys.error(s"Subjects expression missing for non-dichotomous phenotype '${metadata.phenotype}'")
      }
    }
    
    lazy val n: Option[NamedColumnDef[Double]] = nDef match {
      case None => Option(effectiveN)
      case actualN => actualN
    }
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

