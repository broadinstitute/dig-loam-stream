package loamstream.loam.intake

import scala.util.Failure
import scala.util.Try
import scala.util.Success
import loamstream.loam.intake.metrics.BioIndexClient
import loamstream.util.Loggable

/**
 * @author clint
 * 16 Mar, 2021
 */
sealed trait VariantRowExpr[R <: BaseVariantRow] extends TaggedRowParser[VariantRow.Parsed[R]] {
  def metadata: AggregatorMetadata
  
  //TODO: Revisit this
  def metadataWithUploadType: AggregatorMetadata = metadata.copy(topic = Option(uploadType))
  
  def uploadType: UploadType
  
  def markerDef: MarkerColumnDef
  
  def failFast: Boolean
  
  protected def makeRow(row: VariantRow.Tagged): R
  
  override def apply(row: VariantRow.Tagged): VariantRow.Parsed[R] = { 
    def skipped(cause: Option[Failure[VariantRow.Parsed[R]]]) = VariantRow.Skipped(row, aggRowOpt = None, cause = cause)
        
    def transformed(row: VariantRow.Tagged): VariantRow.Transformed[R] = {
      VariantRow.Transformed(
        derivedFrom = row, 
        aggRow = makeRow(row))
    }
    
    if(row.isSkipped) {
      skipped(cause = None)
    } else {
      val attempt: Try[VariantRow.Parsed[R]] = Try(transformed(row))
      
      attempt match {
        case Success(r) => r
        case f @ Failure(_) => if(failFast) attempt.get else attempt.getOrElse(skipped(Some(f)))
      }
    }
  }
  
  protected object metadataColumnDefs {
    val dataset = LiteralColumnExpr(metadata.dataset)
    val phenotype = LiteralColumnExpr(metadata.phenotype)
    val ancestry = LiteralColumnExpr(metadata.ancestry)
  }
}

object VariantRowExpr extends Loggable {
  final case class VariantCountRowExpr(
    metadata: AggregatorMetadata,
    markerDef: MarkerColumnDef,
    alleleCountDef: Option[AnonColumnDef[Long]],
    alleleCountCasesDef: Option[AnonColumnDef[Long]], 
    alleleCountControlsDef: Option[AnonColumnDef[Long]],
    heterozygousCasesDef: Option[AnonColumnDef[Long]],
    heterozygousControlsDef: Option[AnonColumnDef[Long]], 
    homozygousCasesDef: Option[AnonColumnDef[Long]], 
    homozygousControlsDef: Option[AnonColumnDef[Long]],
    failFast: Boolean = false) extends VariantRowExpr[VariantCountRow] {
    
    override def uploadType: UploadType = UploadType.VariantCounts

    override protected def makeRow(row: VariantRow.Tagged): VariantCountRow = {
      VariantCountRow(
        marker = row.marker,
        dataset = metadataColumnDefs.dataset(row),
        phenotype = metadataColumnDefs.phenotype(row),
        ancestry = metadataColumnDefs.ancestry(row),
        alleleCount = alleleCountDef.map(_.apply(row)),
        alleleCountCases = alleleCountCasesDef.map(_.apply(row)), 
        alleleCountControls = alleleCountControlsDef.map(_.apply(row)),
        heterozygousCases = heterozygousCasesDef.map(_.apply(row)), 
        heterozygousControls = heterozygousControlsDef.map(_.apply(row)), 
        homozygousCases = homozygousCasesDef.map(_.apply(row)), 
        homozygousControls = homozygousControlsDef.map(_.apply(row)),
        derivedFromRecordNumber = Some(row.recordNumber))
    }
  }
  
  final class PValueVariantRowExpr private (
    override val metadata: AggregatorMetadata,
    override val markerDef: MarkerColumnDef,
    pvalueDef: ColumnDef[Double],
    zscoreDef: Option[ColumnDef[Double]],
    stderrDef: Option[ColumnDef[Double]],
    betaDef: Option[ColumnDef[Double]],
    oddsRatioDef: Option[ColumnDef[Double]],
    eafDef: Option[ColumnDef[Double]],
    mafDef: Option[ColumnDef[Double]],
    nDef: ColumnDef[Double],
    override val failFast: Boolean) extends VariantRowExpr[PValueVariantRow] {

    override def uploadType: UploadType = UploadType.Variants
    
    override protected def makeRow(row: VariantRow.Tagged):  PValueVariantRow = {
      PValueVariantRow(
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
        n = nDef.apply(row),
        derivedFromRecordNumber = Some(row.recordNumber))
    }
  }
  
  object PValueVariantRowExpr {
    def apply(
      metadata: AggregatorMetadata,
      markerDef: MarkerColumnDef,
      pvalueDef: ColumnDef[Double],
      zscoreDef: Option[ColumnDef[Double]] = None,
      stderrDef: Option[ColumnDef[Double]] = None,
      betaDef: Option[ColumnDef[Double]] = None,
      oddsRatioDef: Option[ColumnDef[Double]] = None,
      eafDef: Option[ColumnDef[Double]] = None,
      mafDef: Option[ColumnDef[Double]] = None,
      casesDef: Option[ColumnDef[Double]] = None,
      controlsDef: Option[ColumnDef[Double]] = None,
      subjectsDef: Option[ColumnDef[Double]] = None,
      nDef: Option[ColumnDef[Double]] = None,
      failFast: Boolean = false,
      bioIndexClient: BioIndexClient = new BioIndexClient.Default()): PValueVariantRowExpr = {
      
      val inferred = OptionalPValueRowColumnDefs(
        metadata = metadata,
        pvalueDef = pvalueDef,
        zscoreDef = zscoreDef,
        stderrDef = stderrDef,
        betaDef = betaDef,
        oddsRatioDef = oddsRatioDef,
        eafDef = eafDef,
        mafDef = mafDef,
        nDef = nDef,
        casesDef = casesDef,
        controlsDef = controlsDef,
        subjectsDef = subjectsDef,
        bioIndexClient = bioIndexClient).inferAll()
  
      new PValueVariantRowExpr(
          metadata = metadata,
          markerDef = markerDef,
          pvalueDef = pvalueDef,
          zscoreDef = inferred.zscoreDef,
          stderrDef = inferred.stderrDef,
          betaDef = inferred.betaDef,
          oddsRatioDef = inferred.oddsRatioDef,
          eafDef = inferred.eafDef,
          mafDef = inferred.mafDef,
          nDef = inferred.nDef,
          failFast = failFast)
    }
  }
  
  private case class PValueRowColumnDefs(
    zscoreDef: Option[ColumnDef[Double]],
    stderrDef: Option[ColumnDef[Double]],
    betaDef: Option[ColumnDef[Double]],
    oddsRatioDef: Option[ColumnDef[Double]],
    eafDef: Option[ColumnDef[Double]],
    mafDef: Option[ColumnDef[Double]],
    nDef: ColumnDef[Double])

  private case class OptionalPValueRowColumnDefs(
    metadata: AggregatorMetadata,
    pvalueDef: ColumnDef[Double],
    zscoreDef: Option[ColumnDef[Double]],
    stderrDef: Option[ColumnDef[Double]],
    betaDef: Option[ColumnDef[Double]],
    oddsRatioDef: Option[ColumnDef[Double]],
    eafDef: Option[ColumnDef[Double]],
    mafDef: Option[ColumnDef[Double]],
    nDef: Option[ColumnDef[Double]],
    casesDef: Option[ColumnDef[Double]],
    controlsDef: Option[ColumnDef[Double]],
    subjectsDef: Option[ColumnDef[Double]],
    bioIndexClient: BioIndexClient) {
    
    private def unchanged(msg: => String): this.type = {
      warn(msg)
      
      this
    }
    
    def inferBeta: OptionalPValueRowColumnDefs = (betaDef, oddsRatioDef) match {
      //TODO: logarithm base?
      case (None, Some(orDef)) => copy(betaDef = Some(orDef.map(scala.math.log))) 
      case _ => unchanged("Can't infer beta without odds ratio")
    }

    def inferOddsRatio: OptionalPValueRowColumnDefs = (oddsRatioDef, betaDef) match {
      case (None, Some(bDef)) => copy(oddsRatioDef = Some(bDef.map(scala.math.exp)))
      case _ => unchanged("Can't infer odds ratio without beta")
    }

    def inferStderr: OptionalPValueRowColumnDefs = (stderrDef, betaDef) match {
      case (None, Some(bDef)) => copy(stderrDef = Some(ColumnDef.combine(bDef, pvalueDef)(Stats.qnorm)))
      case _ => unchanged("Can't infer stderr without beta and pvalue")
    }
    
    def inferZscore: OptionalPValueRowColumnDefs = (zscoreDef, betaDef, stderrDef) match {
      case (None, Some(bDef), Some(seDef)) => copy(zscoreDef = Some(ColumnDef.combine(bDef, seDef)(_ / _)))
      case _ => unchanged("Can't infer zscore without beta and stderr")
    }
    
    def inferMaf: OptionalPValueRowColumnDefs = (mafDef, eafDef) match {
      case (None, Some(eDef)) => copy(mafDef = Some(eDef.map(e => if(e < 0.5) e else 1.0 - e)))
      case _ => unchanged("Can't infer maf without eaf")
    }
    
    private def effectiveN: ColumnDef[Double] = {
      val canonicalPhenotypeOpt = bioIndexClient.findClosestMatch(Phenotype(metadata.phenotype, false))
      
      require(
          canonicalPhenotypeOpt.isDefined, 
          s"Could not find canonical name and dichotomous status for phenotype '${metadata.phenotype}'")
          
      val canonicalPhenotype = canonicalPhenotypeOpt.get
      
      import metadata.phenotype
      
      //if the phenotype is dichotomous, use subjects, else cases+controls
      (canonicalPhenotype.dichotomous, casesDef, controlsDef, subjectsDef) match {
        case (true, Some(cases), Some(controls), _) => {
          info("Computing effective-N from cases and controls")
          
          ColumnDef.combine(cases, controls)(Stats.effectiveN)
        }
        case (true, None, _, _) => sys.error(s"Cases expression missing for dichotomous phenotype '${phenotype}'")
        case (true, _, None, _) => sys.error(s"Controls expression missing for dichotomous phenotype '${phenotype}'")
        case (_, _, _, Some(subjects)) => {
          info("Using subjects expression for effective-N")
          
          subjects
        }
        case _ => sys.error(s"Subjects expression missing for non-dichotomous phenotype '${phenotype}'")
      }
    }
    
    def inferN: OptionalPValueRowColumnDefs = nDef match {
      case None => copy(nDef = Some(effectiveN))
      case _ => this
    }
    
    def inferAll(): PValueRowColumnDefs = {
      val inferenceFns: Seq[OptionalPValueRowColumnDefs => OptionalPValueRowColumnDefs] = Seq(
          _.inferBeta,
          _.inferOddsRatio,
          _.inferStderr,
          _.inferZscore,
          _.inferMaf,
          _.inferN)
          
      inferenceFns.foldLeft(this) { (acc, f) => 
        f(acc)
      }
      
      def msg(column: String) = s"Column '${column}' wasn't defined and couldn't be computed from other columns"
      
      require(nDef.isDefined, msg("n")) 
    
      PValueRowColumnDefs(
        zscoreDef = zscoreDef,
        stderrDef = stderrDef,
        betaDef = betaDef,
        oddsRatioDef = oddsRatioDef,
        eafDef = eafDef,
        mafDef = mafDef,
        nDef = nDef.get)
    }
  }
}