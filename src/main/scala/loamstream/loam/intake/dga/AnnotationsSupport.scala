package loamstream.loam.intake.dga

import java.net.URI

import scala.util.Try

import loamstream.loam.intake.AwsRowSink
import loamstream.loam.intake.Source
import loamstream.util.AwsClient
import loamstream.util.Fold
import loamstream.util.HttpClient
import loamstream.util.HttpClient.Auth
import loamstream.util.Loggable
import loamstream.util.SttpHttpClient
import loamstream.util.TimeUtils
import loamstream.model.Store
import loamstream.loam.intake.CloseablePredicate
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.ToFileLogContext
import loamstream.loam.intake.ConcreteCloseablePredicate
import loamstream.util.CanBeClosed
import loamstream.loam.intake.CloseableTransform
import loamstream.loam.intake.ConcreteCloseableTransform
import scala.util.Success
import loamstream.loam.intake.DataRow
import scala.util.Failure

/**
 * @author clint
 * Jan 21, 2021
 *
 * @see processors/vendors/dga/download.py
 */
trait AnnotationsSupport { self: Loggable with BedSupport with TissueSupport =>
  object Annotations {
    val ingestibleAnnotationTypes: Set[AnnotationType] = Set(
        AnnotationType.AccessibleChromatin, 
        AnnotationType.ChromatinState, 
        AnnotationType.BindingSites)
    
    private final class UploadOps(
        annotation: Annotation,
        auth: Auth,
        awsClient: AwsClient,
        logCtx: ToFileLogContext, 
        yes: Boolean = false) {
      
      import annotation.annotationId 
      import annotation.annotationType
      
      val datasetName: String = toDatasetName(annotation)
      
      val topicName: String = whitespaceToUnderscores(s"annotated_regions")
      
      //create the new dataset
      val sink = new AwsRowSink(
          topic = topicName,
          name = datasetName,
          awsClient = awsClient,
          yes = yes)
      
      val countAndUpload: Fold[BedRow, _, (Int, Unit)] = {
        val doCount: Fold[BedRow, Int, Int] = Fold.count
        
        val doUpload: Fold[BedRow, _, Unit] = Fold.foreach(sink.accept)
        
        doCount |+| doUpload
      }
      
      //create the metadata for this dataset
      val metadata = annotation.toMetadata
      
      def processDownload(download: Annotation.Download): Try[Unit] = {
        import download.url
            
        info(s"Processing ${url}...")
        
        val bedRowExpr = BedRowExpr(annotation)
        
        for {
          rawBedRows <- Beds.downloadBed(url, auth)
        } yield {
          val bedRowAttempts: Source[(DataRow, Try[BedRow])] = rawBedRows.map(row => (row, bedRowExpr(row)))
          
          val bedRows = {
            bedRowAttempts.map(Transforms.logFailures(logCtx)).collect { case (_, Success(bedRow)) => bedRow }
          }
          
          val (count, _) = TimeUtils.time(s"Uploading '${datasetName}'", info(_)) {
            countAndUpload.process(bedRows.records)
          }
          
          info(s"Uploaded ${count} rows to '${datasetName}'")
        }
      }
      
      def commitAndCloseSinkAfter(f: AwsRowSink => Any): Unit = {
        CanBeClosed.using(sink) { sink =>
          f(sink) 
          
          sink.commit(metadata.toJson)
        }
      }
    }
      
    /**
     * Download region annotations loaded from DGA and upload them to S3
     */
    def uploadAnnotatedDataset(
        auth: Auth,
        awsClient: AwsClient,
        logTo: Store, 
        append: Boolean,
        yes: Boolean = false)(annotation: Annotation): Unit = {
      
      uploadAnnotatedDataset(auth, awsClient, IntakeSyntax.Log.toFile(logTo, append), yes)(annotation)
    }
      
    /**
     * Download region annotations loaded from DGA and upload them to S3
     */
    def uploadAnnotatedDataset(
        auth: Auth,
        awsClient: AwsClient,
        logCtx: ToFileLogContext,
        yes: Boolean)(annotation: Annotation): Unit = {
      
      import annotation.annotationId 
      import annotation.annotationType
      
      info(s"Creating ${annotationType} dataset ${annotationId}")
      
      val ops = new UploadOps(annotation, auth, awsClient, logCtx, yes = yes)
      
      ops.commitAndCloseSinkAfter { sink =>
        // if the metadata matches what's in HDFS already it can be skipped
        //TODO: Does this comparison work?  Is it field-order-dependent?
        if(sink.existingMetadata == Option(ops.metadata.toJson)) {
          info(s"Dataset ${ops.datasetName} already up to date; skipping...")
        } else {
          //load each source file
          annotation.downloads.foreach(ops.processDownload)
        }
      }
      
      info(s"Finished uploading ${annotationType} dataset ${annotationId}")
    }
    
    /**
     * Download all the annotations available.
     */
    def downloadAnnotations(
        url: URI = AnnotationsSupport.Defaults.url,
        httpClient: HttpClient = new SttpHttpClient): Source[Try[Annotation]] = {
      
      info(s"Downloading region annotations from '$url' ...")
    
      //fetch all the annotations as JSON
      def resp = TimeUtils.time(s"Hitting $url", info(_)) {
        httpClient.post(
          url = url.toString, 
          headers = Headers.ContentType.applicationJson,
          body = Some("{\"type\": \"Annotation\"}")) //TODO: Make this string with json4s
      }
          
      import org.json4s.jackson.JsonMethods._
          
      def jsonData = resp match {
        case Right(jsonData) => jsonData
        case Left(message) => sys.error(s"Error accessing region annotations: '${message}'")
      }
      
      val (_, tissueSource) = Tissues.versionAndTissueSource()
      
      downloadAnnotations(jsonData, tissueSource)
    }
    
    /**
     * Download all the annotations available.
     */
    def downloadAnnotations(
        annotationJsonString: => String,
        tissueSource: Source[Tissue]): Source[Try[Annotation]] = {
      
      import org.json4s.jackson.JsonMethods._
          
      val annotationJson = parse(annotationJsonString)
      
      val tissueIdsToNames: Map[String, String] = {
        tissueSource.collect { case Tissue(Some(id), Some(name)) => (id, name) }.records.toMap
      }
        
      import Json.JsonOps
      
      //NB: I have no idea what '11' means, if anything, but it's what's present in the JSON from DGA. 
      val jvs = annotationJson.tryAsArray("11").getOrElse(Nil)
        
      Source.FromIterator {
        jvs.iterator.map(Annotation.fromJson(tissueIdsToNames))
      }
    }
    
    private def whitespaceToUnderscores(s: String) = s.toLowerCase.replaceAll("\\s+", "_")
    
    private def toDatasetName(a: Annotation): String = whitespaceToUnderscores(a.annotationId)
    
    object Transforms {
      def logFailures(logTo: Store, append: Boolean = false): CloseableTransform[(DataRow, Try[BedRow])] = {
        logFailures(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def logFailures(implicit logCtx: ToFileLogContext): CloseableTransform[(DataRow, Try[BedRow])] = {
        ConcreteCloseableTransform(logCtx) { 
          case t @ (_, Success(_)) => t
          case t @ (row, Failure(e)) => {
            logCtx.warn(s"Parsing BED row $row failed with ${e.getMessage}", e)
            
            t
          }
        }
      }
    }
    
    object Predicates {
      def bedExists(
          logTo: Store, 
          append: Boolean = false, 
          httpClient: HttpClient = new SttpHttpClient,
          auth: Option[HttpClient.Auth] = None): CloseablePredicate[URI] = {
        
        bedExists(httpClient, auth)(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def bedExists(
          httpClient: HttpClient, 
          auth: Option[HttpClient.Auth])(implicit logCtx: ToFileLogContext): CloseablePredicate[URI] = {
        
        ConcreteCloseablePredicate[URI](logCtx) { uri =>
          val result = Try(httpClient.contentLength(uri.toString, auth)) match {
            case Success(Right(n)) => n > 0
            case _ => false
          }
          
          if(!result) {
            logCtx.warn(s"Could not access '${uri}', skipping it")
          }
           
          result
        }
      }

      def hasAnyBeds(
          logTo: Store, 
          append: Boolean = false, 
          httpClient: HttpClient = new SttpHttpClient,
          auth: Option[HttpClient.Auth] = None): CloseablePredicate[Annotation] = {
        
        hasAnyBeds(httpClient, auth)(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def hasAnyBeds(
          httpClient: HttpClient, 
          auth: Option[HttpClient.Auth])(implicit logCtx: ToFileLogContext): CloseablePredicate[Annotation] = {
        
        val p: CloseablePredicate[URI] = bedExists(httpClient, auth)(logCtx)
        
        ConcreteCloseablePredicate[Annotation](logCtx) { annotation =>
          
          val result = annotation.downloads.iterator.map(_.url).exists(p)
          
          if(!result) {
            logCtx.warn(s"Annotation '${annotation.annotationId}' has no accessible .beds, skipping it")
          }
           
          result
        }
      }
      
      def isUploadable(logTo: Store, append: Boolean = false): CloseablePredicate[Annotation] = {
        isUploadable(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def isUploadable(implicit logCtx: ToFileLogContext): CloseablePredicate[Annotation] = {
        ConcreteCloseablePredicate[Annotation](logCtx) { annotation =>
          val result = annotation.isUploadable
          
          if(!result) {
            logCtx.warn(s"Annotation '${annotation.annotationType}:${annotation.annotationId}' is not uploadable")
          }
           
          result
        }
      }
      
      def hasAnnotationTypeWeCareAbout(logTo: Store, append: Boolean = false): CloseablePredicate[Annotation] = {
        hasAnnotationTypeWeCareAbout(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def hasAnnotationTypeWeCareAbout(implicit logCtx: ToFileLogContext): CloseablePredicate[Annotation] = {
        ConcreteCloseablePredicate[Annotation](logCtx) { annotation =>
          val result = ingestibleAnnotationTypes.contains(annotation.annotationType)
          
          if(!result) {
            logCtx.warn(
                s"Annotation '${annotation.annotationType.name}:${annotation.annotationId}' had type not found in" +
                s"${ingestibleAnnotationTypes.map(_.name).mkString("[",",","]")}")
          }
          
          result
        }
      }
      
      def succeeded(logTo: Store, append: Boolean = false): CloseablePredicate[Try[Annotation]] = {
        succeeded(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def succeeded(implicit logCtx: ToFileLogContext): CloseablePredicate[Try[Annotation]] = {
        ConcreteCloseablePredicate[Try[Annotation]](logCtx) { attempt =>
          val result = attempt.isSuccess
          
          if(!result) {
            //Use recover to succinctly access the underlying exception, just for the logging side-effect.
            attempt.recover { 
              case e => logCtx.warn(s"Couldn't parse annotation due to '${e.getMessage}'", e)
            }
          }
          
          result
        }
      }
    }
  }
}

object AnnotationsSupport {
  object Defaults {
    val url: URI = URI.create("http://www.diabetesepigenome.org:8080/getAnnotation")
  }
}
