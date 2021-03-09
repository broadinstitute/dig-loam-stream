package loamstream.loam.intake

/**
 * @author clint
 * Nov 3, 2020
 */
final case class SourceColumns(
    marker: ColumnName,
    pvalue: ColumnName,
    zscore: Option[ColumnName] = None,
    stderr: Option[ColumnName] = None,
    beta: Option[ColumnName] = None,
    oddsRatio: Option[ColumnName] = None,
    eaf: Option[ColumnName] = None,
    maf: Option[ColumnName] = None,
    n: Option[ColumnName] = None,
    mafCasesControls: Option[ColumnName] = None,
    alleleCountCasesControls: Option[ColumnName] = None,
    alleleCountCases: Option[ColumnName] = None,
    alleleCountControls: Option[ColumnName] = None,
    heterozygousCountCases: Option[ColumnName] = None,
    heterozygousCountControls: Option[ColumnName] = None, 
    homozygousCountCases: Option[ColumnName] = None, 
    homozygousCountControls: Option[ColumnName] = None) {
  
  def withoutZscore: SourceColumns = copy(zscore = None)
  def withoutStderr: SourceColumns = copy(stderr = None)
  def withoutBeta: SourceColumns = copy(beta = None)
  def withoutOddsRatio: SourceColumns = copy(oddsRatio = None)
  def withoutEaf: SourceColumns = copy(eaf = None)
  def withoutMaf: SourceColumns = copy(maf = None)
  def withoutN: SourceColumns = copy(n = None)
  
  def withZscore(newZscore: ColumnName): SourceColumns = copy(zscore = Option(newZscore))
  def withStderr(newStderr: ColumnName): SourceColumns = copy(stderr = Option(newStderr))
  def withBeta(newBeta: ColumnName): SourceColumns = copy(beta = Option(newBeta))
  def withOddsRatio(newOddsRatio: ColumnName): SourceColumns = copy(oddsRatio = Option(newOddsRatio))
  def withEaf(newEaf: ColumnName): SourceColumns = copy(eaf = Option(newEaf))
  def withMaf(newMaf: ColumnName): SourceColumns = copy(maf = Option(newMaf))
  def withN(newN: ColumnName): SourceColumns = copy(n = Option(newN))
  
  def withDefaultZscore: SourceColumns = withZscore(AggregatorColumnNames.zscore)
  def withDefaultStderr: SourceColumns = withStderr(AggregatorColumnNames.stderr)
  def withDefaultBeta: SourceColumns = withBeta(AggregatorColumnNames.beta)
  def withDefaultOddsRatio: SourceColumns = withOddsRatio(AggregatorColumnNames.odds_ratio)
  def withDefaultEaf: SourceColumns = withEaf(AggregatorColumnNames.eaf)
  def withDefaultMaf: SourceColumns = withMaf(AggregatorColumnNames.maf)
  def withDefaultN: SourceColumns = withN(AggregatorColumnNames.n)
  
  private val mapping: Map[ColumnName, ColumnName] = {
    //mandatory columns
    val mandatory = Map(
      AggregatorColumnNames.marker -> this.marker,
      AggregatorColumnNames.pvalue -> this.pvalue)
      
    mandatory ++
      stderr.map(AggregatorColumnNames.stderr -> _) ++
      zscore.map(AggregatorColumnNames.zscore -> _) ++
      beta.map(AggregatorColumnNames.beta -> _) ++
      oddsRatio.map(AggregatorColumnNames.odds_ratio -> _) ++
      eaf.map(AggregatorColumnNames.eaf -> _) ++
      maf.map(AggregatorColumnNames.maf -> _) ++ 
      n.map(AggregatorColumnNames.n -> _) ++
      mafCasesControls.map(AggregatorColumnNames.mafCasesControls -> _) ++
      alleleCountCasesControls.map(AggregatorColumnNames.alleleCountCasesControls -> _) ++
      alleleCountCases.map(AggregatorColumnNames.alleleCountCases -> _) ++
      alleleCountControls.map(AggregatorColumnNames.alleleCountControls -> _) ++
      heterozygousCountCases.map(AggregatorColumnNames.heterozygousCountCases -> _) ++
      heterozygousCountControls.map(AggregatorColumnNames.heterozygousCountControls -> _) ++ 
      homozygousCountCases.map(AggregatorColumnNames.homozygousCountCases -> _) ++
      homozygousCountControls.map(AggregatorColumnNames.homozygousCountControls -> _)
  }
      
  def asConfigFileContents: String = {
    val aggregatorColumnNameToSourceColumnNameLines = mapping.map {
      case (aggregatorColumnName, sourceColumnName) => s"${aggregatorColumnName.name} ${sourceColumnName.name}"
    }
    
    aggregatorColumnNameToSourceColumnNameLines.mkString(System.lineSeparator)
  }
  
  def validate(): Unit = {
    def eafColumn = AggregatorColumnNames.eaf.name
    def mafColumn = AggregatorColumnNames.maf.name
    def betaColumn = AggregatorColumnNames.beta.name
    def oddsRatioColumn = AggregatorColumnNames.odds_ratio.name
    def stderrColumn = AggregatorColumnNames.stderr.name
    def zscoreColumn = AggregatorColumnNames.zscore.name
    
    if(maf.isEmpty) {
      require(eaf.isDefined, s"If ${mafColumn} column is not provided, then ${eafColumn} must be.")
    }
    
    if(beta.isEmpty) {
      require(oddsRatio.isDefined, s"If ${betaColumn} column is not provided, then ${oddsRatioColumn} must be.")
    }
    
    if(oddsRatio.isEmpty) {
      require(beta.isDefined, s"If ${oddsRatioColumn} column is not provided, then ${betaColumn} must be.")
    }
    
    if(stderr.isEmpty) {
      require(beta.isDefined, s"If ${stderrColumn} column is not provided, then ${betaColumn} must be.")
    }
    
    if(zscore.isEmpty) {
      def msg = s"If ${zscoreColumn} column is not provided, then ${betaColumn} and ${stderrColumn} must be."
      
      require(beta.isDefined, msg)
      require(stderr.isDefined, msg)
    }
  }
}

object SourceColumns {
  val defaultMarkerAndPvalueOnly: SourceColumns = {
    SourceColumns(marker = AggregatorColumnNames.marker, pvalue = AggregatorColumnNames.pvalue)
  }
  
  val allColumnsWithDefaultNames: SourceColumns = {
    defaultMarkerAndPvalueOnly.copy(
        zscore = Option(AggregatorColumnNames.zscore),
        stderr = Option(AggregatorColumnNames.stderr),
        beta = Option(AggregatorColumnNames.beta),
        oddsRatio = Option(AggregatorColumnNames.odds_ratio),
        eaf = Option(AggregatorColumnNames.eaf),
        maf = Option(AggregatorColumnNames.maf),
        n = Option(AggregatorColumnNames.n),
        mafCasesControls = Option(AggregatorColumnNames.mafCasesControls),
        alleleCountCasesControls = Option(AggregatorColumnNames.alleleCountCasesControls),
        alleleCountCases = Option(AggregatorColumnNames.alleleCountCases),
        alleleCountControls = Option(AggregatorColumnNames.alleleCountControls),
        heterozygousCountCases = Option(AggregatorColumnNames.heterozygousCountCases),
        heterozygousCountControls = Option(AggregatorColumnNames.heterozygousCountControls), 
        homozygousCountCases = Option(AggregatorColumnNames.homozygousCountCases),
        homozygousCountControls = Option(AggregatorColumnNames.homozygousCountControls))
  }
}
