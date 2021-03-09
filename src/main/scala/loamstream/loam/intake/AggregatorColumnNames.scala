package loamstream.loam.intake


/**
 * @author clint
 * Feb 11, 2020
 */
object AggregatorColumnNames {
  import IntakeSyntax.ColumnNameOps
  
  val marker: ColumnName =     "marker".asColumnName
  val pvalue: ColumnName =     "pvalue".asColumnName
  val zscore: ColumnName =     "zscore".asColumnName
  val stderr: ColumnName =     "stderr".asColumnName
  val beta: ColumnName =       "beta".asColumnName
  val odds_ratio: ColumnName = "odds_ratio".asColumnName
  val eaf: ColumnName =        "eaf".asColumnName
  val maf: ColumnName =        "maf".asColumnName
  val n: ColumnName =          "n".asColumnName
  
  //TODO: What should these new names be?
  val mafCasesControls: ColumnName = "maf_cases_controls".asColumnName
  val alleleCountCasesControls: ColumnName = "allele_count_cases_controls".asColumnName
  val alleleCountCases: ColumnName = "allele_count_cases".asColumnName 
  val alleleCountControls: ColumnName = "allele_count_controls".asColumnName
  val heterozygousCountCases: ColumnName = "heterozygous_count_cases".asColumnName 
  val heterozygousCountControls: ColumnName = "heterozygous_count_controls".asColumnName 
  val homozygousCountCases: ColumnName = "homozygous_count_cases".asColumnName 
  val homozygousCountControls: ColumnName = "homozygous_count_controls".asColumnName 
  
  lazy val values: Seq[ColumnName] = Seq(
      marker,
      pvalue,
      zscore,
      stderr,
      beta,
      odds_ratio,
      eaf,
      maf,
      n,
      mafCasesControls,
      alleleCountCasesControls,
      alleleCountCases, 
      alleleCountControls,
      heterozygousCountCases, 
      heterozygousCountControls, 
      homozygousCountCases, 
      homozygousCountControls)
}
