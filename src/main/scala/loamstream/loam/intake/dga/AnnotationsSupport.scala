package loamstream.loam.intake.dga

import loamstream.loam.intake.Source
import java.net.URI
import loamstream.util.LogContext
import loamstream.util.HttpClient
import loamstream.util.SttpHttpClient
import loamstream.util.Loggable
import scala.util.Success
import scala.util.Failure
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * @author clint
 * Jan 21, 2021
 * 
 * @see processors/vendors/dga/download.py
 */
trait AnnotationsSupport { self: Loggable =>
  
  type AnnotationsField = Annotations => Iterable[Annotation]
  
  val annotationTypes: Map[String, AnnotationsField] = Map(
    "accessible_chromatin" -> (_.accessible_chromatin),
    "chromatin_state" -> (_.chromatin_state),
    "binding_sites" -> (_.binding_sites))
  
  /**
   * Download region annotations loaded from DGA and upload them to S3
   */
  def processAnnotations(assemblyId: String): Unit = { //TODO
    val auth: Auth = ??? //TODO: Secrets()['dga-annotations']

    val annotationsSource = downloadAnnotations(assemblyId) //TODO: url and httpClient
    
    val failed: Buffer[(Annotation, Throwable)] = new ArrayBuffer
    
    //load the bed files
    for {
      annotations <- annotationsSource.records
      (tpe, field) <- annotationTypes
      annotation <- field(annotations).filter(_.isUploadable)
    } {
      try {
        upload_annotated_dataset(annotation, tpe, auth, yes=yes)
      } catch {
        case NonFatal(e) => {
          error(s"Failed to upload dataset ${annotation.annotation_id}: ${e.getMessage}", e)
          
          failed += (annotation -> e)
        }
      }
    }

    //show failures
    for {
      (annotation, e) <- failed
    } {
      error(s"Failed to upload dataset ${annotation.annotation_id}: ${e.getMessage}", e)
    }
  }
  
  def uploadAnnotatedDataset(annotation: Annotation, tpe: String, auth: Auth): Unit = {
    ???
  }
  
  /**
   * Download all the annotations available.
   */
  def downloadAnnotations(
      assemblyId: String = AssemblyIds.hg19,
      url: URI = AnnotationsSupport.Defaults.url,
      httpClient: HttpClient = new SttpHttpClient): Source[Annotations] = {
    
    def annotations = {
      info(s"Downloading region annotations from '$url' ...")
    
      //fetch all the annotations as JSON
      val resp = httpClient.post(
          url = url.toString, 
          headers = Headers.ContentType.applicationJson,
          body = Some("{\"type\": \"Annotation\"}"))
          
      import org.json4s.jackson.JsonMethods._
          
      val json = resp.map(parse(_)) match {
        case Right(jv) => jv
        case Left(message) => sys.error(s"Error accessing region annotations: '${message}'")
      }
      
      Annotations.fromJson(assemblyId)(json) match {
        case Success(as) => as
        case Failure(e) => throw new Exception(s"Error accessing region annotations: ", e) 
      }
    }
    
    Source.producing(annotations)
  }
  
  /*def download_annotations(*argv):
    opts = argparse.ArgumentParser()
    opts.add_argument('--assembly', default='hg19', help='Reference genome assembly to pull (e.g. "GRCh37")')
    opts.add_argument('--url', help='REST end-point URL to DGA annotations')

    # parse arguments
    args = opts.parse_args(argv)

    logging.info('Downloading region annotations...')

    # fetch all the annotations as JSON
    resp = requests.post(
        args.url or _annotations_rest_url,
        headers={'Content-Type': 'application/json'},
        json={'type': 'Annotation'},
    )

    # parse and extract all the fields as kwargs to class constructors
    return Annotations(assembly=args.assembly, **resp.json())*/
}

object AnnotationsSupport {
  object Defaults {
    val url: URI = URI.create("http://www.diabetesepigenome.org:8080/getAnnotation")
  }
}
