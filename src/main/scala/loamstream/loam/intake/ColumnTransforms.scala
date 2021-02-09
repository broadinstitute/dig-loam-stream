package loamstream.loam.intake

import scala.util.matching.Regex

/**
 * @author clint
 * Oct 19, 2020
 */
object ColumnTransforms {
  def ensureAlphabeticChromNames(baseChromExpr: ColumnExpr[String]): ColumnExpr[String] = {
    baseChromExpr.trim.map {
      case "23" => "X"
      case "24" => "Y"
      case "25" => "XY"
      case "26" | "M" | "m" => "MT"
      case rawChrom => rawChrom
    }
  }
  
  def normalizeChromNames(baseChromExpr: ColumnExpr[String]): ColumnExpr[String] = {
    baseChromExpr.trim.map {
      case Regexes.chrom(_, chromosomePart) => chromosomePart.toUpperCase
    }
  }
  
  private object Regexes {
    val chrom: Regex = """(?i)^(chr)?(1\d?|2[0-6]?|[3-9]|x|y|xy|mt?)""".r
  }
}
