package loamstream.loam.intake

import scala.util.Try

import com.typesafe.config.Config

import AggregatorMetadata.Defaults
import loamstream.conf.ConfigParser
import loamstream.util.Options

/**
 * @author clint
 * Feb 11, 2020
 */
final case class AggregatorMetadata(
    dataset: String,
    phenotype: String,
    varIdFormat: String = Defaults.varIdFormat,
    ancestry: Ancestry,
    author: Option[String] = None,
    tech: TechType,
    quantitative: Option[AggregatorMetadata.Quantitative],
    properties: Seq[(String, String)] = Nil) {
    
  require(AggregatorVarIdFormat.isValid(varIdFormat))
  
  def subjects: Option[Int] = quantitative.map(_.subjects)
  
  def asConfigFileContents: String = {
    import AggregatorMetadata.escape
    
    val authorPart = author.map(a => s"author ${escape(a)}").getOrElse("")
    
    import AggregatorMetadata.Quantitative.CasesAndControls
    import AggregatorMetadata.Quantitative.Subjects
    import java.lang.System.lineSeparator

    val quantitativePart = quantitative match {
      case Some(CasesAndControls(cases, controls)) => s"cases ${cases}${lineSeparator}controls ${controls}"
      case Some(Subjects(s)) => s"subjects ${s}"
      case _ => ""
    }
    
    s"""|dataset ${dataset} ${phenotype}
        |ancestry ${escape(ancestry.name)}
        |tech ${escape(tech.name)}
        |${quantitativePart}
        |var_id ${varIdFormat}
        |${authorPart}""".stripMargin.trim
  }
}

object AggregatorMetadata extends ConfigParser[AggregatorMetadata] {

  /**
   * Wrap strings containing whitespace in double quotes, escaping any existing double-quote characters.
   */
  def escape(s: String): String = {
    //flatMapping feels odd, but it works.  Figuring out the right combination of escapes in order to use 
    //String.replaceAll (which takes a regex as a Java/Scala string as its first arg, requiring its own escaping)
    //was obviously possible, but not worth the required frustration for such a small, 
    //relatively-infrequently-called method.    
    def withQuotesEscaped = s.flatMap {
      case '\"' => "\\\""
      case c => c.toString
    }
    
    if(s.exists(_.isWhitespace)) s""""${withQuotesEscaped}"""" else s
  }
  
  sealed trait Quantitative {
    def subjects: Int
  }
  
  object Quantitative {
    final case class CasesAndControls(cases: Int, controls: Int) extends Quantitative {
      override def subjects: Int = cases + controls
    }
    
    final case class Subjects(value: Int) extends Quantitative {
      override def subjects: Int = value
    }
  }
  
  private final case class Parsed(
    dataset: String,
    phenotype: Option[String] = None,
    varIdFormat: String = Defaults.varIdFormat,
    ancestry: String,
    author: Option[String] = None,
    tech: String,
    cases: Option[Int] = None,
    controls: Option[Int] = None,
    subjects: Option[Int] = None) {
    
    private def quantitative: Option[Quantitative] = (cases, controls, subjects) match {
      case (Some(cas), Some(cntrls), None) => Some(Quantitative.CasesAndControls(cas, cntrls))
      case (None, None, Some(subjects)) => Some(Quantitative.Subjects(subjects))
      case _ => None
    }
    
    def toMetadata: Try[AggregatorMetadata] = {
      for {
        ph <- Options.toTry(phenotype)("Expected phenotype to be present")
        tt <- TechType.tryFromString(tech)
        an <- Ancestry.tryFromString(ancestry) 
      } yield {
        AggregatorMetadata(
          dataset = dataset,
          phenotype = ph,
          varIdFormat = varIdFormat,
          ancestry = an,
          author = author,
          tech = tt,
          quantitative = quantitative)
      }
    }
    
    def toNoPhenotype: Try[NoPhenotype] = {
      for {
        tt <- TechType.tryFromString(tech)
        an <- Ancestry.tryFromString(ancestry)
      } yield {
        NoPhenotype(
          dataset = dataset,
          varIdFormat = varIdFormat,
          ancestry = an,
          author = author,
          tech = tt,
          quantitative = quantitative)
      }
    }
    
    def toNoPhenotypeOrQuantitative: Try[NoPhenotypeOrQuantitative] = {
      for {
        tt <- TechType.tryFromString(tech)
        an <- Ancestry.tryFromString(ancestry)
      } yield {
        NoPhenotypeOrQuantitative(
          dataset = dataset,
          varIdFormat = varIdFormat,
          ancestry = an,
          author = author,
          tech = tt)
      }
    }
  }
  
  override def fromConfig(config: Config): Try[AggregatorMetadata] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    
    //NB: Marshal the contents of loamstream.intake.metadata into a Metadata instance.
    //Names of fields in Metadata and keys under loamstream.intake.metadata must match.
    Try(config.as[Parsed](Defaults.configKey)).flatMap(_.toMetadata)
  }
  
  object Defaults {
    val configKey: String = "loamstream.aggregator.intake.metadata"
    val varIdFormat: String = "{chrom}_{pos}_{ref}_{alt}"
  }
  
  final case class NoPhenotype(
      dataset: String,
      varIdFormat: String = Defaults.varIdFormat,
      ancestry: Ancestry,
      author: Option[String],
      tech: TechType,
      quantitative: Option[Quantitative]) {
    
    def toMetadata(phenotype: String): AggregatorMetadata = {
      AggregatorMetadata(
          dataset, 
          phenotype, 
          varIdFormat, 
          ancestry, 
          author, 
          tech, 
          quantitative = quantitative)
    }
  }
  
  object NoPhenotype extends ConfigParser[NoPhenotype] {
    override def fromConfig(config: Config): Try[NoPhenotype] = {
      import net.ceedubs.ficus.Ficus._
      import net.ceedubs.ficus.readers.ArbitraryTypeReader._
      
      //NB: Marshal the contents of loamstream.intake.metadata into a Metadata instance.
      //Names of fields in Metadata and keys under loamstream.intake.metadata must match.
      Try(config.as[Parsed](Defaults.configKey)).flatMap(_.toNoPhenotype)
    }
  }
  
  final case class NoPhenotypeOrQuantitative(
      dataset: String,
      varIdFormat: String = Defaults.varIdFormat,
      ancestry: Ancestry,
      author: Option[String],
      tech: TechType) {
    
    def toMetadata(phenotype: String, quantitative: Option[Quantitative]): AggregatorMetadata = {
      AggregatorMetadata(
          dataset, 
          phenotype, 
          varIdFormat, 
          ancestry, 
          author, 
          tech, 
          quantitative = quantitative)
    }
  }
  
  object NoPhenotypeOrQuantitative extends ConfigParser[NoPhenotypeOrQuantitative] {
    override def fromConfig(config: Config): Try[NoPhenotypeOrQuantitative] = {
      import net.ceedubs.ficus.Ficus._
      import net.ceedubs.ficus.readers.ArbitraryTypeReader._
      
      //NB: Marshal the contents of loamstream.intake.metadata into a Metadata instance.
      //Names of fields in Metadata and keys under loamstream.intake.metadata must match.
      Try(config.as[Parsed](Defaults.configKey)).flatMap(_.toNoPhenotypeOrQuantitative)
    }
  }
}
