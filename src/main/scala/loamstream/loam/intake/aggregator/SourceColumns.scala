package loamstream.loam.intake.aggregator

import loamstream.loam.intake.ColumnName

final case class SourceColumns(
    marker: ColumnName,
    pvalue: ColumnName,
    zscore: Option[ColumnName] = None,
    stderr: Option[ColumnName] = None,
    beta: Option[ColumnName] = None,
    oddsRatio: Option[ColumnName] = None,
    eaf: Option[ColumnName] = None,
    maf: Option[ColumnName] = None) {
  
  def withoutZscore: SourceColumns = copy(zscore = None)
  def withoutStderr: SourceColumns = copy(stderr = None)
  def withoutBeta: SourceColumns = copy(beta = None)
  def withoutOddsRatio: SourceColumns = copy(oddsRatio = None)
  def withoutEaf: SourceColumns = copy(eaf = None)
  def withoutMaf: SourceColumns = copy(maf = None)
  
  def withZscore(newZscore: ColumnName): SourceColumns = copy(zscore = Option(newZscore))
  def withStderr(newStderr: ColumnName): SourceColumns = copy(stderr = Option(newStderr))
  def withBeta(newBeta: ColumnName): SourceColumns = copy(beta = Option(newBeta))
  def withOddsRatio(newOddsRatio: ColumnName): SourceColumns = copy(oddsRatio = Option(newOddsRatio))
  def withEaf(newEaf: ColumnName): SourceColumns = copy(eaf = Option(newEaf))
  def withMaf(newMaf: ColumnName): SourceColumns = copy(maf = Option(newMaf))
  
  def withDefaultZscore: SourceColumns = withZscore(ColumnNames.zscore)
  def withDefaultStderr: SourceColumns = withStderr(ColumnNames.stderr)
  def withDefaultBeta: SourceColumns = withBeta(ColumnNames.beta)
  def withDefaultOddsRatio: SourceColumns = withOddsRatio(ColumnNames.odds_ratio)
  def withDefaultEaf: SourceColumns = withEaf(ColumnNames.eaf)
  def withDefaultMaf: SourceColumns = withMaf(ColumnNames.maf)
  
  private val mapping: Map[ColumnName, ColumnName] = {
    //mandatory columns
    val mandatory = Map(
      ColumnNames.marker -> this.marker,
      ColumnNames.pvalue -> this.pvalue)
      
    mandatory ++
      stderr.map(ColumnNames.stderr -> _) ++
      zscore.map(ColumnNames.zscore -> _) ++
      beta.map(ColumnNames.beta -> _) ++
      oddsRatio.map(ColumnNames.odds_ratio -> _) ++
      eaf.map(ColumnNames.eaf -> _) ++
      maf.map(ColumnNames.maf -> _)
  }
      
  def asConfigFileContents: String = {
    val aggregatorColumnNameToSourceColumnNameLines = mapping.map {
      case (aggregatorColumnName, sourceColumnName) => s"${aggregatorColumnName.name} ${sourceColumnName.name}"
    }
    
    aggregatorColumnNameToSourceColumnNameLines.mkString(System.lineSeparator)
  }
  
  def validate(): Unit = {
    if(maf.isEmpty) {
      require(eaf.isDefined, s"If ${ColumnNames.maf} column is not provided, then ${ColumnNames.eaf} must be.")
    }
    
    if(beta.isEmpty) {
      require(
          oddsRatio.isDefined, 
          s"If ${ColumnNames.beta} column is not provided, then ${ColumnNames.odds_ratio} must be.")
    }
    
    if(oddsRatio.isEmpty) {
      require(
          beta.isDefined,
          s"If ${ColumnNames.odds_ratio} column is not provided, then ${ColumnNames.beta} must be.")
    }
    
    if(stderr.isEmpty) {
      require(
          beta.isDefined,
          s"If ${ColumnNames.stderr} column is not provided, then ${ColumnNames.beta} must be.")
    }
    
    if(zscore.isEmpty) {
      def msg = {
        s"If ${ColumnNames.zscore} column is not provided, then ${ColumnNames.beta} and ${ColumnNames.stderr} must be."
      }
      
      require(beta.isDefined, msg)
      require(stderr.isDefined, msg)
    }
  }
}

object SourceColumns {
  val defaultMarkerAndPvalueOnly: SourceColumns = {
    SourceColumns(marker = ColumnNames.marker, pvalue = ColumnNames.pvalue)
  }
  
  val allColumnsWithDefaultNames: SourceColumns = {
    defaultMarkerAndPvalueOnly.copy(
        zscore = Option(ColumnNames.zscore),
        stderr = Option(ColumnNames.stderr),
        beta = Option(ColumnNames.beta),
        oddsRatio = Option(ColumnNames.odds_ratio),
        eaf = Option(ColumnNames.eaf),
        maf = Option(ColumnNames.maf))
  }
}
