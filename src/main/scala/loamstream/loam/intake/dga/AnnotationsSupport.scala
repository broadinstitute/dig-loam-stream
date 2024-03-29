package loamstream.loam.intake.dga

import java.net.URI

import scala.util.Try

import loamstream.loam.intake.AwsRowSink
import loamstream.loam.intake.Source
import loamstream.util.S3Client
import loamstream.util.Fold
import loamstream.util.HttpClient
import loamstream.util.HttpClient.Auth
import loamstream.util.Loggable
import loamstream.util.DefaultHttpClient
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
import org.json4s.JsonAST.JValue
import loamstream.util.Tries
import loamstream.util.Terminable

/**
 * @author clint
 * Jan 21, 2021
 *
 * @see processors/vendors/dga/download.py
 */
trait AnnotationsSupport { self: Loggable with BedSupport with TissueSupport =>
  object Annotations {
    private[dga] final class UploadOps(
        annotation: Annotation,
        auth: Option[Auth],
        awsClient: S3Client,
        //TODO: Should be just a LogContext
        logCtx: ToFileLogContext) {
      
      import annotation.annotationId 
      import annotation.annotationType
      
      //TODO: TEST!
      val datasetName: String = toDatasetName(annotation)
      
      //TODO: TEST!
      val topicName: String = whitespaceToUnderscores(s"annotated_regions/${annotation.category.name}")
      
      //create the new dataset
      private val sink = new AwsRowSink(
          topic = topicName,
          dataset = datasetName,
          techType = None,
          phenotype = None,
          metadata = annotation.toMetadata.toJson,
          s3Client = awsClient)
      
      private val countAndUpload: Fold[BedRow, _, (Int, Unit)] = {
        val doCount: Fold[BedRow, Int, Int] = Fold.count
        
        val doUpload: Fold[BedRow, _, Unit] = Fold.foreach(sink.accept)
        
        doCount |+| doUpload
      }
      
      //create the metadata for this dataset
      private val metadata = annotation.toMetadata
      
      private def processDownload(download: Annotation.Download): Unit = {
        import download.url
        
        info(s"Processing ${url}...")
        
        val bedRowExpr = BedRowExpr(annotation)
        
        // add headers default: Option[Seq[String]] = Some(Seq("chrom", "start", "end", "state", "value"))
		val (handle, bedRowAttempts) = annotation.annotationType match {
          case AnnotationType.VariantToGene => Beds.downloadBed(url, auth, Some(Seq("variant","gene","score","distanceToTSS","evidence","cS2G","info","chr","location")))
          case AnnotationType.GeneExpression => Beds.downloadBed(url, auth, Some(Seq("gene","ensemblID","#samples","TPM_for_all_samples","min_TPM","1stQu_TPM","median_TPM","mean_TPM","3rdQu_TPM","max_TPM")))
          case _ => Beds.downloadBed(url, auth)
        }

        val rowTuples = bedRowAttempts.map(row => (row, bedRowExpr(row)))
        
        val bedRows = rowTuples
          .map(Transforms.logFailures(logCtx))
          .collect { case (_, Success(bedRow)) => bedRow }

        //NB: Fail fast if process() fails
        val (Success((count, _)), elapsedMillis) = TimeUtils.elapsed {
          handle.stopAfter { 
            countAndUpload.process(bedRows.records) 
          }
        }
        
        info {
          val rowsPerSecond = count.toDouble / (elapsedMillis.toDouble / 1000.0)
          
          s"Uploaded '${datasetName}' with ${count} rows in ${elapsedMillis}ms (${rowsPerSecond} rows/s)"
        }
      }
      
      def processDownloads(): Unit = {
        CanBeClosed.using(sink) { sink =>
          // if the metadata matches what's in S3 already it can be skipped
          //TODO: Does this comparison work?  Is it field-order-dependent?
          if(sink.existingMetadata == Option(metadata.toJson)) {
            info(s"Dataset ${datasetName} already up to date; skipping...")
          } else {
            //load each source file
            annotation.downloads.foreach(processDownload)
          }
        }
      }
    }
      
    /**
     * Download region annotations loaded from DGA and upload them to S3
     */
    def uploadAnnotatedDataset(
        auth: Option[Auth],
        awsClient: S3Client,
        logTo: Store, 
        append: Boolean)(annotation: Annotation): Unit = {
      
      uploadAnnotatedDataset(auth, awsClient, IntakeSyntax.Log.toFile(logTo, append))(annotation)
    }
      
    /**
     * Download region annotations loaded from DGA and upload them to S3
     */
    def uploadAnnotatedDataset(
        auth: Option[Auth],
        awsClient: S3Client,
        logCtx: ToFileLogContext)(annotation: Annotation): Unit = {
      
      import annotation.annotationId 
      import annotation.annotationType
      
      info(s"Creating ${annotationType} dataset ${annotationId}")
      
      val ops = new UploadOps(annotation, auth, awsClient, logCtx)
      
      ops.processDownloads()
      
      info(s"Finished uploading ${annotationType} dataset ${annotationId}")
    }
    
    /**
     * Download all the annotations available.
     */
    def downloadAnnotations(
        url: URI,
        httpClient: HttpClient = new DefaultHttpClient): Source[Try[Annotation]] = {
      
      info(s"Downloading region annotations from '$url' ...")
    
      //fetch all the annotations as JSON
      //body removed: body = Some("{\"type\": \"Annotation\"}")
      def resp = TimeUtils.time(s"Hitting $url", info(_)) {
        httpClient.post(
          url = url.toString, 
          headers = Headers.ContentType.applicationJson,
          body = None) //TODO: Make this string with json4s
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
      
      //NB: I have no idea what '17' means, if anything, but it's what's present in the JSON from DGA. 
      val jvs: Iterable[JValue] = {
        val topLevelObjectAttempt = annotationJson.tryAsObject
        
        topLevelObjectAttempt.flatMap { topLevelObj =>
          val valueAttempts: Iterable[Try[Seq[JValue]]] = topLevelObj.values.iterator.map(_.tryAsArray).toList
          
          Tries.sequence(valueAttempts).map(_.flatten)
        }.get //:(, fail fast
      }
        
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
          httpClient: HttpClient = new DefaultHttpClient,
          auth: Option[HttpClient.Auth] = None): CloseablePredicate[URI] = {
        
        bedExists(httpClient, auth)(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def bedExists(
          httpClient: HttpClient, 
          auth: Option[HttpClient.Auth])(implicit logCtx: ToFileLogContext): CloseablePredicate[URI] = {
        
        ConcreteCloseablePredicate[URI](logCtx) { uri =>
          val attempt = Try(httpClient.contentLength(uri.toString, auth))

          val result = attempt match {
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
          httpClient: HttpClient = new DefaultHttpClient,
          auth: Option[HttpClient.Auth] = None): CloseablePredicate[Annotation] = {
        
        hasAnyBeds(httpClient, auth)(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def hasAnyBeds(
          httpClient: HttpClient, 
          auth: Option[HttpClient.Auth])(implicit logCtx: ToFileLogContext): CloseablePredicate[Annotation] = {
        
        val p: CloseablePredicate[URI] = bedExists(httpClient, auth)(logCtx)
        
        ConcreteCloseablePredicate[Annotation](logCtx, p) { annotation =>
          
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
      
      def hasAnnotationType(
          annotationTypes: Set[AnnotationType],
          logTo: Store, 
          append: Boolean = false): CloseablePredicate[Annotation] = {
        
        hasAnnotationType(annotationTypes)(IntakeSyntax.Log.toFile(logTo, append))
      }
      
      def hasAnnotationType(
          annotationTypes: Set[AnnotationType])
         (implicit logCtx: ToFileLogContext): CloseablePredicate[Annotation] = {
        
        ConcreteCloseablePredicate[Annotation](logCtx) { annotation =>
          val result = annotationTypes.contains(annotation.annotationType)
          
          if(!result) {
            logCtx.warn(
                s"Annotation '${annotation.annotationType.name}:${annotation.annotationId}' had type not found in" +
                s"${annotationTypes.map(_.name).mkString("[",",","]")}")
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
    val url: URI = URI.create("http://cmdga.org:6543/getAnnotation")
  }
}
