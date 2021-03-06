package loamstream.loam.intake.dga

import loamstream.util.Loggable
import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths
import scala.util.Try
import loamstream.util.Tries
import scala.util.Success
import loamstream.util.LogContext
import org.json4s._
import scala.collection.compat._

/**
 * @author clint
 * Jan 15, 2021
 * 
 * A single annotation. A base class used by all annotations.
 * 
 * @see Annotation in processors/vendors/dga/download.py
 */
final case class Annotation private[dga] (
    annotationType: AnnotationType,
    annotationId: String,
    category: Option[String],
    tissueId: Option[String],
    tissue: Option[String],
    source: Option[String],
    assay: Option[Seq[String]],
    collection: Option[Seq[String]],
    biosampleId: String,
    biosampleType: String,
    biosample: Option[String],
    method: Option[String], 
    portalUsage: String,
    harmonizedStates: Option[Map[String, String]],
    downloads: Seq[Annotation.Download]) extends Loggable {
  
  /**
   * Returns True if the annotation meets all criteria for ingesting.
   */
  def isUploadable: Boolean = {
    val anyDownloads = downloads.nonEmpty
    val portalUsageIsAcceptable = !portalUsageIsNone
    
    //ignore any datasets with no no valid datasets to load and portalUsage != "None"
    val result = {
      anyDownloads && 
      portalUsageIsAcceptable
    }
    
    if(!result) {
      def msg(specificPart: String) = {
        s"Skipping ${annotationId}: biosample id: ${biosampleId} " +
        s"because ${specificPart} ; downloads: ${downloads}"
      }
      
      if(!anyDownloads) {
        warn(msg("No bed files were available"))
      } else if(!portalUsageIsAcceptable) {
        warn(msg(s"portal_usage field is '${portalUsage}'"))
      }
    }
    
    result
  }
  
  def notUploadable: Boolean = !isUploadable
  
  private[dga] def portalUsageIsNone: Boolean = portalUsage == "None"
  
  def toMetadata: Annotation.Metadata = Annotation.Metadata(
      sources = this.downloads,
      annotationMethod = this.method) 
}

object Annotation {
  import Json.JsonOps
  
  private[dga] def spacesToUnderscores(s: String): String = s.replaceAll("\\s+", "_")
  
  private def allFileDownloads(json: JValue): Try[Iterable[Download]] = {
    json.tryAsObject("file_download").flatMap { downloadsById =>
      val downloadJVs = downloadsById.values.collect { case arr: JArray => arr }.flatMap(_.arr)
      
      Tries.sequence(downloadJVs.map(Download.fromJson))
    }
  }
    
  private def filteredSortedFileDownloads(
      annotationId: String, 
      json: JValue)(implicit ctx: LogContext): Try[Seq[Download]] = {
    
    allFileDownloads(json).map(_.filter(isValidDownload(annotationId)).to(Seq).sortBy(_.md5Sum))
  }
  
  def fromJson(
      tissueIdsToNames: Map[String, String])(json: JValue)(implicit ctx: LogContext): Try[Annotation] = {
    
    for {
      annotationId <- json.tryAsString("annotation_id")
      annotationType <- json.tryAsString("annotation_type").flatMap(AnnotationType.tryFromString)
      fileDownloads <- filteredSortedFileDownloads(annotationId, json)
      biosampleId <- json.tryAsString("biosample_term_id")
      biosampleType <- json.tryAsString("biosample_type")
      portalUsage <- json.tryAsString("portal_usage")
      method = json.asStringOption("annotation_method")
      collections = json.tryAsStringArray("collection_tags").toOption
      assay = json.tryAsStringArray("underlying_assay").toOption
      source = json.asStringOption("annotation_source")
      category = json.asStringOption("annotation_category")
      tissueId = json.asStringOption("portal_tissue_id")
      biosample = tissueIdsToNames.get(biosampleId)
      tissue = tissueId.flatMap(tissueIdsToNames.get)
      harmonizedStates = json.tryAs[Map[String, String]]("harmonized_states").toOption
    } yield {
      Annotation(
        annotationId = annotationId,
        annotationType = annotationType,
        category = category,
        tissueId = tissueId,
        tissue = tissue,
        source = source,
        assay = assay,
        collection = collections,
        biosampleId = biosampleId,
        biosampleType = biosampleType,
        biosample = biosample,
        method = method,
        portalUsage = portalUsage,
        harmonizedStates = harmonizedStates,
        downloads = fileDownloads)
    }
  }
  
  //only keep files of the right format, assembly and have been released
  private[dga] def isValidDownload(
      annotationId: String)(download: Annotation.Download)(implicit ctx: LogContext): Boolean = {
    
    def fileName: String = s"${annotationId}/${download.file}"
    
    def isReleased: Boolean = {
      val released = download.status.countsAsReleased
      
      if(!released) {
        ctx.warn(s"File ${fileName} is not released; skipping...")
      }
      
      released
    }
    
    def isBed: Boolean = {
      val bed = File.isBed(download.file)
      
      if(!bed) {
        ctx.warn(s"File ${fileName} is not a BED file; skipping...")
      }
      
      bed
    }
    
    def assemblyMatchesHg19: Boolean = {
      
      val hg19 = AssemblyIds.hg19
      
      val asmsMatch = AssemblyMap.matchAssemblies(download.assemblyId, hg19)
      
      if(!asmsMatch) {
        ctx.warn(s"File ${fileName} with assembly Id '${download.assemblyId}' " +
                 s"does not match assembly ${hg19}; skipping...")
      }
      
      asmsMatch
    }
    
    isReleased && isBed && assemblyMatchesHg19 
  }
  
  final case class Download private (assemblyId: String, url: URI, file: Path, status: Status, md5Sum: String) {
    //TODO: Automate this
    def toJson: JValue = JObject(
        "assemblyId" -> JString(assemblyId),
        "url" -> JString(url.toString),
        "file" -> JString(file.toString),
        "status" -> JString(status.toString),
        "md5Sum" -> JString(md5Sum))
  }
  
  object Download {
    private[dga] def apply(assemblyId: String, url: URI, status: Status, md5Sum: String): Download = {
      Download(
          assemblyId = assemblyId, 
          url = url, 
          file = Paths.get(url.getPath), 
          status = status, 
          md5Sum = md5Sum)
    }
    
    def fromJson(json: JValue): Try[Download] = {
      for {
        url <- json.tryAsString("files_href").map(URI.create(_))
        assemblyId <- json.tryAsString("files_assembly")
        status <- json.tryAsString("files_status").map(_.toLowerCase).map(Status.fromString)
        md5Sum <- json.tryAsString("files_md5sum")
      } yield Download(
          assemblyId = assemblyId, 
          url = url, 
          status = status, 
          md5Sum = md5Sum)
    }
  }
  
  sealed abstract class Status(val name: String) {
    def countsAsReleased: Boolean = this match {
      case Status.Released | Status.Uploading => true
      case _ => false
    }
    
    override def toString: String = name
  }
  
  object Status {
    case object Released extends Status("released")
    case object Uploading extends Status("uploading")
    case object Other extends Status("other")
    
    def fromString(s: String): Status = s.toLowerCase match {
      case Released.name => Released
      case Uploading.name => Uploading
      case _ => Other
    }
  }
  
  object File {
    def isBed(file: Path): Boolean = {
      val asString = file.toString
      
      asString.endsWith(".bed") || asString.endsWith(".bed.gz")
    }
    
    def notBed(file: Path): Boolean = !isBed(file)
  }
  
  final case class Metadata(
    sources: Seq[Annotation.Download],
    annotationMethod: Option[String]) {
        
    def vendor: String = "DGA"
    
    def version: String = "1.0"
    
    //TODO: Automate this
    def toJson: JObject = {
      val annotationMethodPart: Option[JField] = annotationMethod.map("method" -> JString(_))
      
      val fields: Seq[JField] = Seq(
        "sources" -> JArray(this.sources.to(List).map(_.toJson))
      ) ++ annotationMethodPart
      
      JObject(fields: _*)
    }
  }
}
