package loamstream.loam.intake.aggregator

/**
 * @author clint
 * Feb 11, 2020
 */
final case class Metadata(
    dataset: String,
    phenotype: String,
    varIdFormat: Option[String],
    ancestry: String,
    author: Option[String],
    tech: String,
    cases: Int,
    controls: Int) {
    
  varIdFormat.foreach(f => require(VarIdFormat.isValid(f)))
  
  def subjects: Int = cases + controls
  
  def asConfigFileContents: String = {
    val authorPart = author.map(a => s"author ${a}").getOrElse("")
    
    val varIdFormatPart = varIdFormat.map(f => s"var_id ${f}").getOrElse("")
    
    s"""|dataset ${dataset} ${phenotype}
        |ancestry ${ancestry}
        |tech ${tech}
        |cases ${cases}
        |controls ${controls}
        |subjects ${subjects}
        |${varIdFormatPart}
        |${authorPart}""".stripMargin
  }
}
