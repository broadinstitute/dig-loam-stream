package loamstream.loam.intake.metrics

import loamstream.util.Loggable
import loamstream.loam.intake.Variant

/**
 * @author clint
 * Mar 18, 2020
 */
trait BioIndexClient {
  def isKnown(varId: String): Boolean
  
  final def isUnknown(varId: String): Boolean = !isKnown(varId)
}

object BioIndexClient {
  object Defaults {
    val baseUrl: String = "http://ec2-18-215-38-136.compute-1.amazonaws.com:5000/api/query/Variants"
    
    def httpClient: HttpClient = new SttpHttpClient
  }
  
  final class Default(
      baseUrl: String = Defaults.baseUrl,
      httpClient: HttpClient = Defaults.httpClient) extends BioIndexClient with Loggable {
    
    override def isKnown(varId: String): Boolean = isKnown(Variant.from(varId))
  
    def isKnown(variant: Variant): Boolean = {
      info(s"Looking up ${variant.colonDelimited}")
    
      val url = s"${baseUrl}?q=${variant.asFullBioIndexCoord}"
    
      val contentLengthE = httpClient.contentLength(url) 
    
      contentLengthE match {
        case Left(errorMessage) => sys.error(errorMessage)
        case Right(l) => l > 0
      }
    }
  }
}
