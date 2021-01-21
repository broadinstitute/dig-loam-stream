package loamstream.loam.intake.dga

import loamstream.util.Loggable
import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths
import scala.util.Try
import loamstream.util.Tries
import scala.util.Success
import loamstream.util.LogContext

/**
 * @author clint
 * Jan 15, 2021
 * 
 * A single annotation. A base class used by all annotations.
 * 
 * @see Annotation in processors/vendors/dga/download.py
 */
final case class Annotation private[dga] (
    assembly: String,
    annotation_id: String,
    biosample_type: String,
    biosample_id: String,
    biosample_name: String,
    system_slims: String,
    organ_slims: String, 
    dbxrefs: String,
    harmonized_states: String,
    annotation_method: String,
    //kwargs: Map[String, Any],
    file_download: Seq[Annotation.Download]) extends Loggable {
  
  /**
   * Returns True if the annotation meets all criteria for ingesting.
   */
  def isUploadable: Boolean = {
    //if annot.annotation_id is None:
    //    return False
    //if annot.biosample_id is None:
    //    return False

    //ignore any datasets with no valid datasets to load
    file_download.nonEmpty
  }
}

object Annotation {
  import org.json4s._
  import Json.JsonOps
  
  def fromJson(assembly: String)(json: JValue)(implicit ctx: LogContext): Try[Annotation] = {
    def allFileDownloads: Try[Iterable[Download]] = {
      json.tryAsObject("file_download").flatMap { downloadsById =>
        val downloadJVs = downloadsById.values.collect { case arr: JArray => arr }.flatMap(_.arr)
        
        Tries.sequence(downloadJVs.map(Download.fromJson(assembly)))
      }
    }
    
    for {
      annotation_id <- json.tryAsString("annotation_id")
      biosample_type <- json.tryAsString("biosample_type")
      biosample_id <- json.tryAsString("biosample_id")
      biosample_name <- json.tryAsString("biosample_name")
      system_slims <- json.tryAsString("system_slims")
      organ_slims <- json.tryAsString("organ_slims") 
      dbxrefs <- json.tryAsString("dbxrefs")
      harmonized_states <- json.tryAsString("harmonized_states")
      annotation_method <- json.tryAsString("annotation_method")
      fileDownloads <- allFileDownloads.map(_.filter(isValidDownload(assembly, annotation_id)).toSeq.sortBy(_.md5Sum))
    } yield {
      Annotation(
        assembly = assembly,
        annotation_id = annotation_id,
        biosample_type = biosample_type,
        biosample_id = biosample_id,
        biosample_name = biosample_name,
        system_slims = system_slims,
        organ_slims = organ_slims, 
        dbxrefs = dbxrefs,
        harmonized_states = harmonized_states,
        annotation_method = annotation_method,
        file_download = fileDownloads)
    }
  }
  
  //only keep files of the right format, assembly and have been released
  private def isValidDownload(
      assembly: String, 
      annotation_id: String)(download: Annotation.Download)(implicit ctx: LogContext): Boolean = {
    
    def fileName: String = s"${annotation_id}/${download.file}"
    
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
    
    def assembliesMatch: Boolean = {
      val asmsMatch = AssemblyMap.match_assemblies(download.assembly, assembly)
      
      if(!asmsMatch) {
        ctx.warn(s"File ${fileName} does not match assembly ${assembly}; skipping...")
      }
      
      asmsMatch
    }
    
    isReleased && isBed && assembliesMatch 
    
    /*if(!download.status.countsAsReleased) {
      ctx.warn(s"File ${fileName} is not released; skipping...")
      
      false
    } else if(File.notBed(download.file)) {
      ctx.warn(s"File ${fileName} is not a BED file; skipping...")
        
      false
    } else if(!AssemblyMap.match_assemblies(download.assembly, assembly)) {
      ctx.warn(s"File ${fileName} does not match assembly ${assembly}; skipping...")
      
      false
    } else {
      true
    }*/
  }
  
  final case class Download private (assembly: String, url: URI, file: Path, status: Status, md5Sum: String) {
    //def url: URI = URI.create(href)//urllib.parse.urlparse(download.get('href', ''))
    //def file: Path = Paths.get(url.getPath).getFileName//os.path.basename(url.path)
    //def sstatus: String = status.toLowerCase//download.get('status', '').lower()
  }
  
  object Download {
    def fromJson(assembly: String)(json: JValue): Try[Download] = {
      for {
        url <- json.tryAsString("files_href").map(URI.create(_))
        status <- json.tryAsString("files_status").map(_.toLowerCase).map(Status.fromString)
        md5Sum <- json.tryAsString("files_md5sum")
        file = Paths.get(url.getPath)
      } yield Download(assembly, url, file, status, md5Sum)
    }
  }
  
  sealed trait Status {
    def countsAsReleased: Boolean = this match {
      case Status.Released | Status.Uploading => true
      case _ => false
    }
  }
  
  object Status {
    case object Released extends Status
    case object Uploading extends Status
    case object Other extends Status
    
    def fromString(s: String): Status = s match {
      case "released" => Released
      case "uploading" => Uploading
      case _ => Other
    }
  }
  
  private object Regexes {
    val bedOrBedGz = "\\.bed(\\.gz)?$".r
  }
  
  object File {
    def isBed(file: Path): Boolean = file.toString match {
      case Regexes.bedOrBedGz() => true
      case _ => false
    }
    
    def notBed(file: Path): Boolean = !isBed(file)
  }
}
