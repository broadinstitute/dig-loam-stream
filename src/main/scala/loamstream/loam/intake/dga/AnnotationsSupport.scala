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
import org.broadinstitute.dig.aws.config.Secrets
import scala.util.Try
import loamstream.loam.intake.AwsRowSink
import loamstream.loam.intake.RowSink
import loamstream.util.AwsClient
import loamstream.util.HttpClient.Auth
import loamstream.util.Maps
import loamstream.util.TimeUtils
import loamstream.loam.intake.metrics.Metrics
import loamstream.loam.intake.metrics.Metric
import loamstream.util.Fold
import org.json4s.JsonAST.JValue

/**
 * @author clint
 * Jan 21, 2021
 * 
 * @see processors/vendors/dga/download.py
 */
trait AnnotationsSupport { self: Loggable with BedSupport with DgaSyntax =>
  
  type AnnotationsField = Annotations => Iterable[Annotation]
  
  val annotationTypes: Map[String, AnnotationsField] = Map(
    "accessible_chromatin" -> (_.accessible_chromatin),
    "chromatin_state" -> (_.chromatin_state),
    "binding_sites" -> (_.binding_sites))
  
  /**
   * Download region annotations loaded from DGA and upload them to S3
   */
  def uploadAnnotatedDataset(
      annotation: Annotation, 
      tpe: String, 
      auth: Auth,
      awsClient: AwsClient,
      yes: Boolean = false): Unit = {
    
    import annotation.annotationId 
    
    info(s"Creating ${tpe} dataset ${annotationId}")
    
    val datasetName = toDatasetName(tpe)
    
    //create the new dataset
    val sink = new AwsRowSink(
        topic = "annotated_regions",
        name = datasetName,
        awsClient = awsClient,
        yes = yes)
    
    //create the metadata for this dataset
    val metadata = annotation.toMetadata
    
    // if the metadata matches what's in HDFS already it can be skipped
    //TODO: Does this comparison work?  Is it field-order-dependent?
    if(sink.existingMetadata == Option(metadata.toJson)) {
      info(s"Dataset ${datasetName} already up to date; skipping...")
    } else {
      //load each source file
      for {
        download <- annotation.downloads
      } {
        import download.url
        
        info(s"Processing ${url}...")
        
        val bedRowExpr = BedRowExpr(annotation = annotation, annotationType = tpe)
        
        val bedRows = downloadBed(url, auth).map(bedRowExpr)
        
        val doCount: Fold[BedRow, Int, Int] = Fold.count
        
        val doUpload: Fold[BedRow, _, Unit] = Fold.foreach(sink.accept)
        
        val f: Fold[BedRow, _, (Int, Unit)] = doCount |+| doUpload
        
        val (count, _) = f.process(bedRows.records)
        
        info(s"Uploaded $count rows")
      }
    }
  }
  
  /**
   * Download all the annotations available.
   */
  def downloadAnnotations(
      assemblyId: String = AssemblyIds.hg19,
      url: URI = AnnotationsSupport.Defaults.url,
      httpClient: HttpClient = new SttpHttpClient): Source[Annotation] = {
    
    info(s"Downloading region annotations from '$url' ...")
  
    //fetch all the annotations as JSON
    val resp = TimeUtils.time(s"Hitting $url", info(_)) {
      httpClient.post(
        url = url.toString, 
        headers = Headers.ContentType.applicationJson,
        body = Some("{\"type\": \"Annotation\"}")) //TODO: Make this string with json4s
    }
        
    import org.json4s.jackson.JsonMethods._
        
    val json = resp.map(parse(_)) match {
      case Right(jv) => jv
      case Left(message) => sys.error(s"Error accessing region annotations: '${message}'")
    }
    
    val (_, tissueSource) = Dga.versionAndTissueSource()
    
    val tissueIdsToNames: Map[String, String] = {
      tissueSource.collect { case Tissue(Some(id), Some(name)) => (id, name) }.records.toMap
    }
      
    import Json.JsonOps
    
    //TODO: 11?
    val jvs = json.tryAsArray("11").getOrElse(Nil)
      
    //TODO: Don't drop failures, log them at a minimum
    //Tries.sequence(jvs.map(Annotation.fromJson(assemblyId, tissueIdsToNames)))
    Source.FromIterator {
      jvs.iterator.map(Annotation.fromJson(assemblyId, tissueIdsToNames)).collect { case Success(a) => a }
    }
  }
  
  private def toDatasetName(s: String): String = s.toLowerCase.split("\\s+").mkString("_")
}

object AnnotationsSupport {
  object Defaults {
    val url: URI = URI.create("http://www.diabetesepigenome.org:8080/getAnnotation")
  }
}
