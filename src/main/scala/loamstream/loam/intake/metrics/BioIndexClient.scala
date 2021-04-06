package loamstream.loam.intake.metrics

import loamstream.util.Loggable
import loamstream.loam.intake.Variant
import loamstream.loam.intake.Dataset
import loamstream.loam.intake.Phenotype
import org.json4s.jackson.JsonMethods.parse
import org.json4s._
import loamstream.util.Iterators
import loamstream.loam.intake.HasName
import loamstream.util.HttpClient
import loamstream.util.SttpHttpClient
import loamstream.util.Traversables
import loamstream.util.Maps

/**
 * @author clint
 * Mar 18, 2020
 */
trait BioIndexClient {
  def isKnown(variant: Variant): Boolean
  
  def isKnown(dataset: Dataset): Boolean
  
  def isKnown(phenotype: Phenotype): Boolean
  
  final def isUnknown(variant: Variant): Boolean = !isKnown(variant)
  
  final def isUnknown(dataset: Dataset): Boolean = !isKnown(dataset)
  
  final def isUnknown(phenotype: Phenotype): Boolean = !isKnown(phenotype)
  
  def findClosestMatch(dataset: Dataset): Option[Dataset]
  
  def findClosestMatch(phenotype: Phenotype): Option[Phenotype]
}

object BioIndexClient {
  object Defaults {
    val baseUrl: String = "http://ec2-18-215-38-136.compute-1.amazonaws.com:5000/api"
    
    def httpClient: HttpClient = new SttpHttpClient
  }
  
  final class Default(
      baseUrl: String = Defaults.baseUrl,
      httpClient: HttpClient = Defaults.httpClient) extends BioIndexClient with Loggable {
    
    debug(s"Instantiating default BioIndexClient pointing at '$baseUrl'")
    
    private val variantsBaseUrl = s"${baseUrl}/query/Variants"
    private val datasetsBaseUrl = s"${baseUrl}/portal/datasets"
    private val phenotypesBaseUrl = s"${baseUrl}/portal/phenotypes"
    
    override def isKnown(variant: Variant): Boolean = {
      info(s"Looking up ${variant.colonDelimited}")
    
      val url = s"${variantsBaseUrl}?q=${variant.asFullBioIndexCoord}"
    
      val contentLengthE = httpClient.contentLength(url) 
    
      contentLengthE match {
        case Left(errorMessage) => sys.error(errorMessage)
        case Right(l) => l > 0
      }
    }
    
    /**
     * Case-sensitive lookup of dataset by name
     */
    override def isKnown(dataset: Dataset): Boolean = {
      info(s"Looking up dataset ${dataset}...")
      
      datasetsByName.contains(dataset.name)
    }
    
    /**
     * Case-sensitive lookup of phenotype by name
     */
    override def isKnown(phenotype: Phenotype): Boolean = {
      info(s"Looking up phenotype ${phenotype}...")
      
      phenotypesByName.contains(phenotype.name)
    }
    
    override def findClosestMatch(dataset: Dataset): Option[Dataset] = {
      doFindClosestMatch(datasetsByNormalizedName)(dataset)
    }
  
    override def findClosestMatch(phenotype: Phenotype): Option[Phenotype] = {
      doFindClosestMatch(phenotypesByNormalizedName)(phenotype)
    }
  
    import Traversables.Implicits.TraversableOps
    import Maps.Implicits.MapOps
    
    private lazy val datasetsByName: Map[String, Dataset] = getNames(datasetsBaseUrl).map(Dataset(_)).mapBy(_.name)
    
    //Normalized (upper cased) dataset names mapped to real dataset names, to allow case-insensitive mapping
    //of mis-capitalized names to canonical ones.
    private lazy val datasetsByNormalizedName: Map[String, Dataset] = datasetsByName.mapKeys(_.toUpperCase)
    
    private lazy val phenotypesByName: Map[String, Phenotype] = {
      getNamesToDichotomousFlags(phenotypesBaseUrl).map(Phenotype.tupled).mapBy(_.name)
    }
    
    private lazy val phenotypesByNormalizedName: Map[String, Phenotype] = phenotypesByName.mapKeys(_.toUpperCase)
    
    private def doFindClosestMatch[A <: HasName](knownNames: Map[String, A])(lookingFor: A): Option[A] = {
      knownNames.get(lookingFor.name.toUpperCase) match {
        case matchOpt @ Some(_) => matchOpt
        case None => sys.error(s"Found no case-insensitive matches for '${lookingFor.name}'.")
      }
    }
    
    private def getNamesToDichotomousFlags(url: String): Iterator[(String, Boolean)] = {
      getDataElements(url).iterator.map { jv =>
        (jv, jv \ "name", jv \ "dichotomous")
      }.zipWithIndex.map {
        case ((_, JString(name), JInt(dichotomous)), _) => (name, dichotomous != 0)
        case ((jv, _, _), i) => sys.error(s"Couldn't parse 'data' array element at index ${i} in offending json '$jv'")
      }
    }
    
    private def getNames(url: String): Iterator[String] = {
      getDataElements(url).iterator.map(_ \ "name").zipWithIndex.map {
        case (JString(name), _) => name
        case (_, i) => sys.error(s"Couldn't parse 'data' array element at index ${i}")
      }
    }
    
    private def getDataElements(url: String): Iterable[JValue] = {
      val contentsE = httpClient.get(url)
      
      contentsE match {
        case Left(errorMessage) => sys.error(errorMessage)
        case Right(body) => {
          val json = parse(body)
          
          (json \ "data") match {
            case JArray(elems) => elems
            case _ => sys.error("Couldn't parse 'data' element as a JSON array")
          }
        }
      }
    }
  }
}
