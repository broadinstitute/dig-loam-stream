package loamstream.loam.intake.metrics

import loamstream.util.Loggable
import loamstream.loam.intake.Variant
import loamstream.loam.intake.aggregator.Dataset
import loamstream.loam.intake.aggregator.Phenotype
import org.json4s.jackson.JsonMethods.parse
import org.json4s._
import loamstream.util.Iterators
import loamstream.loam.intake.aggregator.HasName

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
    
    override def isKnown(dataset: Dataset): Boolean = {
      info(s"Looking up dataset ${dataset}...")
      
      datasetNames.contains(dataset.name)
    }
    
    override def isKnown(phenotype: Phenotype): Boolean = {
      info(s"Looking up phenotype ${phenotype}...")
      
      phenotypeNames.contains(phenotype.name)
    }
    
    override def findClosestMatch(dataset: Dataset): Option[Dataset] = {
      doFindClosestMatch(datasetNames, Dataset(_))(dataset)
    }
  
    override def findClosestMatch(phenotype: Phenotype): Option[Phenotype] = {
      doFindClosestMatch(phenotypeNames, Phenotype(_))(phenotype)
    }
  
    private def datasetNames: Iterator[String] = getNames(datasetsBaseUrl)
    
    private def phenotypeNames: Iterator[String] = getNames(phenotypesBaseUrl)
    
    private def doFindClosestMatch[A <: HasName](
        knownNames: Iterator[String], 
        constructor: String => A)(lookingFor: A): Option[A] = {
      
      val matches = knownNames.filter(_.toUpperCase == lookingFor.name.toUpperCase)
      
      import Iterators.Implicits._
      
      val firstMatchOpt = matches.nextOption()
      
      val noRemainingMatches = matches.isEmpty
      
      if(noRemainingMatches) { firstMatchOpt.map(constructor) }
      else {
        val remainingMatches = matches
        
        sys.error(s"Found ${remainingMatches.size + 1} case-insensitive matches for '${lookingFor.name}'." +  
                   "Please choose one and try again")
      }
    }
    
    private def getNames(url: String): Iterator[String] = {
      val contentsE = httpClient.get(url)
      
      contentsE match {
        case Left(errorMessage) => sys.error(errorMessage)
        case Right(body) => {
          val json = parse(body)
          
          (json \ "data") match {
            case JArray(elems) => {
              elems.iterator.map(_ \ "name").zipWithIndex.map {
                case (JString(name), _) => name
                case (_, i) => sys.error(s"Couldn't parse 'data' array element at index ${i}")
              }
            }
            case _ => sys.error("Couldn't parse 'data' element as a JSON array")
          }
        }
      }
    }
  }
}
