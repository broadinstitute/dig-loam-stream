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
    category: AnnotationCategory,
    tissueId: Option[String],
    tissue: Option[String],
    source: Option[String],
    assay: Option[Seq[String]],
    collection: Option[Seq[String]],
    diseaseTermId: Option[String],
    diseaseTermName: Option[String],
    biosampleId: Option[String],
    biosampleType: Option[String],
    biosample: Option[String],
    method: Option[String], 
    portalUsage: Option[String], //TODO: Optional since it comes back null sometimes, but technically required. 
    harmonizedStates: Option[Map[String, String]],
    downloads: Seq[Annotation.Download],
    private val derivedFrom: Option[JValue] = None) extends Loggable {
  
  /**
   * Returns True if the annotation meets all criteria for ingesting.
   */
  def isUploadable: Boolean = {
    val anyDownloads = downloads.nonEmpty
    
    //Only datasets with 1+ valid downloads are ingestible
    val ingestible = anyDownloads
    
    if(!ingestible) {
      def msg(specificPart: String) = {
        s"Skipping ${annotationId}: biosample id: ${biosampleId} " +
        s"because ${specificPart} ; downloads: ${downloads}"
      }
      
      if(!ingestible) {
        warn(msg("No bed files were available"))
      } 
    }
    
    ingestible
  }
  
  def notUploadable: Boolean = !isUploadable
  
  def toMetadata: Annotation.Metadata = {
    require(derivedFrom.isDefined, s"Couldn't make metadata for annotation ${annotationId}: missing source JSON")
    
    Annotation.Metadata(
      annotationMethod = this.method,
      derivedFrom = derivedFrom.get) 
  }
}

object Annotation {
  import Json.JsonOps
  
  private def allFileDownloads(json: JValue): Try[Iterable[Download]] = {
    json.tryAsObject("file_download").flatMap { downloadsById =>
      val downloadJVs = downloadsById.values.collect { case arr: JArray => arr }.flatMap(_.arr)
      
      Tries.sequence(downloadJVs.map(Download.fromJson))
    }
  }
    
  private def filteredSortedFileDownloads(
      annotationId: String,
      category: AnnotationCategory,
      json: JValue)(implicit ctx: LogContext): Try[Seq[Download]] = {
    
    allFileDownloads(json).map(_.filter(isValidDownload(annotationId, category)).to(Seq).sortBy(_.md5Sum))
  }
  
  def fromJson(
      tissueIdsToNames: Map[String, String])(json: JValue)(implicit ctx: LogContext): Try[Annotation] = {
    
    for {
      annotationId <- json.tryAsString("annotation_id")
      annotationType <- json.tryAsString("annotation_type").flatMap(AnnotationType.tryFromString)
      category <- json.tryAsString("annotation_category").flatMap(AnnotationCategory.tryFromString)
      fileDownloads <- filteredSortedFileDownloads(annotationId, category, json)
      biosampleId = json.asStringOption("biosample_term_id")
      biosampleType = json.asStringOption("biosample_type")
      portalUsage = json.asStringOption("portal_usage")
      method = json.asStringOption("annotation_method")
      collections = json.tryAsStringArray("collection_tags").toOption
      diseaseTermId = json.asStringOption("disease_term_id")
      diseaseTermName = json.asStringOption("disease_term_name")
      assay = json.tryAsStringArray("underlying_assay").toOption //required?
      source = json.asStringOption("annotation_source")
      tissueId = json.asStringOption("portal_tissue_id")
      tissue = tissueId.flatMap(tissueIdsToNames.get)
      biosample = biosampleId.flatMap(tissueIdsToNames.get)//tissueIdsToNames.get(biosampleId)
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
        diseaseTermId = diseaseTermId,
        diseaseTermName = diseaseTermName,
        biosampleType = biosampleType,
        biosample = biosample,
        method = method,
        portalUsage = portalUsage,
        harmonizedStates = harmonizedStates,
        downloads = fileDownloads,
        derivedFrom = Option(json))
    }
  }
  
  //only keep files of the right format, assembly and have been released
  private[dga] def isValidDownload(
      annotationId: String,
      category: AnnotationCategory)(download: Annotation.Download)(implicit ctx: LogContext): Boolean = {
    
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

    def isTsv: Boolean = {
      val tsv = File.isTsv(download.file)
      
      if(!tsv) {
        ctx.warn(s"File ${fileName} is not a TSV file; skipping...")
      }
      
      tsv
    }

    def assemblyIsValid: Boolean = {
      
      val hg19 = AssemblyIds.hg19
      val grch38 = AssemblyIds.grch38

      val asmsMatch = Seq(AssemblyMap.matchAssemblies(download.assemblyId, hg19), AssemblyMap.matchAssemblies(download.assemblyId, grch38)).exists(y => y == true)

      if(!asmsMatch) {
        ctx.warn(s"File ${fileName} with assembly Id '${download.assemblyId}' " +
                 s"does not match valid assemblies: ${hg19}, ${grch38}; skipping...")
      }
      
      asmsMatch
    }

    category match {
      case AnnotationCategory.GeneExpressionLevels => isReleased && isTsv && assemblyIsValid
      case _ => isReleased && isBed && assemblyIsValid
    }
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
    
    def values: Set[Status] = Set(Released, Uploading, Other)
  }
  
  object File {
    def isBed(file: Path): Boolean = {
      val asString = file.toString
      
      asString.endsWith(".bed") || asString.endsWith(".bed.gz")
    }
    def isTsv(file: Path): Boolean = {
      val asString = file.toString
      
      asString.endsWith(".tsv") || asString.endsWith(".tsv.gz")
    }
    
    def notBed(file: Path): Boolean = !isBed(file)
    def notTsv(file: Path): Boolean = !isTsv(file)
  }
  
  final case class Metadata(
    annotationMethod: Option[String],
    derivedFrom: JValue) {
        
    def vendor: String = "DGA"
    
    def version: String = "1.0"
    
    //TODO: Automate this
    def toJson: JObject = {
      val annotationMethodPart: Option[JField] = annotationMethod.map("method" -> JString(_))
      
      val fields: Seq[JField] = Seq(
          "vendor" -> JString(vendor),
          "version" -> JString(version),
          "derivedFrom" -> derivedFrom
      ) ++ annotationMethodPart
      
      JObject(fields: _*)
    }
  }
}
