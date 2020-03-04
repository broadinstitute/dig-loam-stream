package loamstream.loam.intake.aggregator

import loamstream.loam.intake.ColumnName

final case class SourceColumns(
    marker: ColumnName,
    pValue: ColumnName,
    zScore: Option[ColumnName] = None,
    stderr: Option[ColumnName] = None,
    beta: Option[ColumnName] = None,
    oddsRatio: Option[ColumnName] = None,
    eaf: Option[ColumnName] = None,
    maf: Option[ColumnName] = None) {
  
  validate()
  
  private val mapping: Map[ColumnName, ColumnName] = {
    //mandatory columns
    val mandatory = Map(
      ColumnNames.marker -> this.marker,
      ColumnNames.pvalue -> this.pValue)
      
    mandatory ++
      stderr.map(ColumnNames.stderr -> _) ++
      zScore.map(ColumnNames.zscore -> _) ++
      beta.map(ColumnNames.beta -> _) ++
      oddsRatio.map(ColumnNames.odds_ratio -> _) ++
      eaf.map(ColumnNames.eaf -> _) ++
      maf.map(ColumnNames.maf -> _)
  }
      
  def asConfigFileContents: String = {
    val aggregatorColumnNameToSourceColumnNameLines = for {
      aggregatorColumnName <- ColumnNames.values
    } yield {
      val sourceColumnName = mapping(aggregatorColumnName)
      
      s"${aggregatorColumnName.name} ${sourceColumnName.name}"
    }
    
    aggregatorColumnNameToSourceColumnNameLines.mkString(System.lineSeparator)
  }
  
  private def validate(): Unit = {
    if(maf.isEmpty) {
      require(eaf.isDefined)
    }
    
    if(beta.isEmpty) {
      require(oddsRatio.isDefined)
    }
    
    if(oddsRatio.isEmpty) {
      require(beta.isDefined)
    }
    
    if(stderr.isEmpty) {
      require(beta.isDefined)
    }
    
    if(zScore.isEmpty) {
      require(beta.isDefined)
    }
  }
}
