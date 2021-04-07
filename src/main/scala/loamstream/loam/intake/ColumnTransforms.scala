package loamstream.loam.intake

import scala.util.matching.Regex

/**
 * @author clint
 * Oct 19, 2020
 */
object ColumnTransforms {
  private def doEnsureAlphabeticChromNames(s: String): String = s.trim match {
    case "23" => "X"
    case "24" => "Y"
    case "25" => "XY"
    case "26" | "M" | "m" => "MT"
    case rawChrom => rawChrom
  }
  
  def ensureAlphabeticChromNames(baseChromExpr: ColumnExpr[String]): ColumnExpr[String] = {
    baseChromExpr.map(doEnsureAlphabeticChromNames)
  }
  
  def ensureAlphabeticChromNamesOpt(baseChromExpr: ColumnExpr[Option[String]]): ColumnExpr[Option[String]] = {
    baseChromExpr.map(_.map(doEnsureAlphabeticChromNames))
  }
  
  private def doNormalizeChromNames(s: String): String = s.trim match {
    case Regexes.chrom(_, chromosomePart) => chromosomePart.toUpperCase
  }
  
  def normalizeChromNames(baseChromExpr: ColumnExpr[String]): ColumnExpr[String] = {
    baseChromExpr.map(doNormalizeChromNames)
  }
  
  def normalizeChromNamesOpt(baseChromExpr: ColumnExpr[Option[String]]): ColumnExpr[Option[String]] = {
    baseChromExpr.map(_.map(doNormalizeChromNames))
  }
  
  private def doNormalizeSpaces(requireNonEmpty: Boolean)(s: String): String = {
    val trimmed = s.trim
    
    if(requireNonEmpty) { 
      require(trimmed.nonEmpty)
    }
    
    trimmed.replaceAll("\\s+", "_")
  }
  
  def normalizeSpaces(
      baseExpr: ColumnExpr[String], 
      requireNonEmpty: Boolean = false): ColumnExpr[String] = baseExpr.map(doNormalizeSpaces(requireNonEmpty))
  
  def normalizeSpacesOpt(
      baseExpr: ColumnExpr[Option[String]], 
      requireNonEmpty: Boolean = false): ColumnExpr[Option[String]] = {
    
    baseExpr.map(_.map(doNormalizeSpaces(requireNonEmpty)))
  }
  
  private object Regexes {
    val chrom: Regex = """(?i)^(chr)?(1\d?|2[0-6]?|[3-9]|x|y|xy|mt?)""".r
  }
}
