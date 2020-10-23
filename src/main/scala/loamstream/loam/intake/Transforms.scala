package loamstream.loam.intake

/**
 * @author clint
 * Oct 19, 2020
 */
object Transforms {
  def ensureAlphabeticChromNames(baseChromExpr: ColumnExpr[String]): ColumnExpr[String] = {
    baseChromExpr.map(_.trim).map {
      case "23" => "X"
      case "24" => "Y"
      case "25" => "XY"
      case "26" => "MT"
      case rawChrom => rawChrom
    }
  }
}
