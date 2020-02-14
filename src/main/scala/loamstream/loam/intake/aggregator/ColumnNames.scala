package loamstream.loam.intake.aggregator

import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.ColumnName

/**
 * @author clint
 * Feb 11, 2020
 */
object ColumnNames {
  import IntakeSyntax.ColumnNameOps
  
  val marker: ColumnName =     "marker".asColumnName
  val pvalue: ColumnName =     "pvalue".asColumnName
  val zscore: ColumnName =     "zscore".asColumnName
  val stderr: ColumnName =     "stderr".asColumnName
  val beta: ColumnName =       "beta".asColumnName
  val odds_ratio: ColumnName = "odds_ratio".asColumnName
  val eaf: ColumnName =        "eaf".asColumnName
  val maf: ColumnName =        "maf".asColumnName
  
  lazy val values: Seq[ColumnName] = Seq(
      marker,
      pvalue,
      zscore,
      stderr,
      beta,
      odds_ratio,
      eaf,
      maf)
}
