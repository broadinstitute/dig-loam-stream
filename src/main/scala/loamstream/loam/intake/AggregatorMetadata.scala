package loamstream.loam.intake

import scala.util.Try

import com.typesafe.config.Config

import AggregatorMetadata.Defaults
import loamstream.conf.ConfigParser
import loamstream.util.Options
import org.json4s.JsonAST.JValue
import org.json4s.JsonAST.JString
import org.json4s.JsonAST.JObject

/**
 * @author clint
 * Feb 11, 2020
 */
final case class AggregatorMetadata(
    bucketName: String,
    topic: Option[UploadType],
    dataset: String,
    phenotype: String,
    varIdFormat: String = Defaults.varIdFormat,
    ancestry: Ancestry,
    author: Option[String] = None,
    tech: TechType,
    sex: Option[Sex] = None,
    quantitative: Option[AggregatorMetadata.Quantitative],
    properties: Seq[(String, String)] = Nil) {
    
  require(AggregatorVarIdFormat.isValid(varIdFormat))
  
  def subjects: Option[Int] = quantitative.map(_.subjects)
  
  def asMetadataFileContents: String = {
    import java.lang.System.lineSeparator
    
    toTuples.map { case (name, value) => s"${name} ${value}" }.mkString(lineSeparator)
  }
  
  def asJson: Seq[(String, JValue)] = {
    import java.lang.System.lineSeparator
    
    toTuples.map { case (name, value) => (name, JString(value)) }
  }
  
  def asJObject: JObject = JObject(asJson: _*)
  
  private def toTuples: Seq[(String, String)] = {
    import AggregatorMetadata.escape
    
    val authorPart: Seq[(String, String)] = author.map(a => Seq("author" -> escape(a))).getOrElse(Nil)
    
    import AggregatorMetadata.Quantitative.CasesAndControls
    import AggregatorMetadata.Quantitative.Subjects
    import java.lang.System.lineSeparator

    val quantitativePart: Seq[(String, String)] = quantitative match {
      case Some(CasesAndControls(cases, controls)) => Seq("cases" -> cases.toString, "controls" -> controls.toString)
      case Some(Subjects(s)) => Seq("subjects" -> s.toString)
      case _ => Nil
    }

    val sexPart: Seq[(String, String)] = sex.map(s => Seq("sex" -> s.toString)).getOrElse(Nil)
    
    val s3Uri: String = {
      //TODO: 
      require(topic.isDefined, "Couldn't determine S3 path due to missing topic")
      
      val path = AwsRowSink.makePath(
        topic = topic.get.name, 
        dataset = dataset, 
        techType = Option(tech), 
        phenotype = Option(phenotype), 
        baseDir = None)
      
      s"s3://${bucketName}/${path}"
    }
    
    Seq(
      "uri" -> s3Uri,
      "dataset" -> s"${dataset} ${phenotype}",
      "ancestry" -> escape(ancestry.name),
      "tech" -> escape(tech.name)) ++
      sexPart ++
      quantitativePart ++
      authorPart
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
    bucketName: String = Defaults.bucketName,
    topic: Option[String] = None,
    dataset: String,
    phenotype: Option[String] = None,
    varIdFormat: String = Defaults.varIdFormat,
    ancestry: String,
    author: Option[String] = None,
    tech: String,
    sex: Option[String] = None,
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
        ut = topic.flatMap(UploadType.fromString)
        s = sex.flatMap(Sex.fromString)
      } yield {
        AggregatorMetadata(
          bucketName = bucketName,
          topic = ut,
          dataset = dataset,
          phenotype = ph,
          varIdFormat = varIdFormat,
          ancestry = an,
          author = author,
          tech = tt,
          sex = s,
          quantitative = quantitative)
      }
    }
    
    def toNoPhenotype: Try[NoPhenotype] = {
      for {
        tt <- TechType.tryFromString(tech)
        an <- Ancestry.tryFromString(ancestry)
        ut = topic.flatMap(UploadType.fromString)
        s = sex.flatMap(Sex.fromString)
      } yield {
        NoPhenotype(
          bucketName = bucketName,
          topic = ut, 
          dataset = dataset,
          varIdFormat = varIdFormat,
          ancestry = an,
          author = author,
          tech = tt,
          sex = s,
          quantitative = quantitative)
      }
    }
    
    def toNoPhenotypeOrQuantitative: Try[NoPhenotypeOrQuantitative] = {
      for {
        tt <- TechType.tryFromString(tech)
        an <- Ancestry.tryFromString(ancestry)
        ut = topic.flatMap(UploadType.fromString)
        s = sex.flatMap(Sex.fromString)
      } yield {
        NoPhenotypeOrQuantitative(
          bucketName = bucketName,
          topic = ut,
          dataset = dataset,
          varIdFormat = varIdFormat,
          ancestry = an,
          author = author,
          tech = tt,
          sex = s)
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
    val bucketName: String = "dig-analysis-data"
  }
  
  final case class NoPhenotype(
      bucketName: String,
      topic: Option[UploadType],
      dataset: String,
      varIdFormat: String = Defaults.varIdFormat,
      ancestry: Ancestry,
      author: Option[String],
      tech: TechType,
      sex: Option[Sex],
      quantitative: Option[Quantitative]) {
    
    def toMetadata(phenotype: String): AggregatorMetadata = {
      AggregatorMetadata(
          bucketName = bucketName,
          topic = topic,
          dataset = dataset, 
          phenotype = phenotype , 
          varIdFormat = varIdFormat, 
          ancestry = ancestry, 
          author = author, 
          tech = tech,
          sex = sex,
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
      bucketName: String,
      topic: Option[UploadType],
      dataset: String,
      varIdFormat: String = Defaults.varIdFormat,
      ancestry: Ancestry,
      author: Option[String],
      tech: TechType,
      sex: Option[Sex]) {
    
    def toMetadata(phenotype: String, quantitative: Option[Quantitative]): AggregatorMetadata = {
      AggregatorMetadata(
          bucketName = bucketName,
          topic = topic,
          dataset = dataset, 
          phenotype = phenotype, 
          varIdFormat = varIdFormat, 
          ancestry = ancestry, 
          author = author, 
          tech = tech,
          sex = sex,
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
