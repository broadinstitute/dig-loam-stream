package loamstream.loam.intake.aggregator

import loamstream.loam.intake.ColumnName

final case class SourceColumns(
    marker: ColumnName,
    pValue: ColumnName,
    zScore: ColumnName,
    stderr: ColumnName,
    beta: ColumnName,
    oddsRatio: ColumnName,
    eaf: ColumnName,
    maf: ColumnName) {
  
  private val mapping: Map[ColumnName, ColumnName] = Map(
      ColumnNames.marker -> this.marker,
      ColumnNames.pvalue -> this.pValue,
      ColumnNames.zscore -> this.zScore,
      ColumnNames.stderr -> this.stderr,
      ColumnNames.beta -> this.beta,
      ColumnNames.odds_ratio -> this.oddsRatio,
      ColumnNames.eaf -> this.eaf,
      ColumnNames.maf -> this.maf)
      
  def asConfigFileContents: String = {
    val aggregatorColumnNameToSourceColumnNameLines = for {
      aggregatorColumnName <- ColumnNames.values
    } yield {
      val sourceColumnName = mapping(aggregatorColumnName)
      
      s"${aggregatorColumnName.name} ${sourceColumnName.name}"
    }
    
    aggregatorColumnNameToSourceColumnNameLines.mkString(System.lineSeparator)
  }
}
