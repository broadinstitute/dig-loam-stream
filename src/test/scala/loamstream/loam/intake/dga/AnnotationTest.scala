package loamstream.loam.intake.dga

import org.scalatest.FunSuite
import loamstream.TestHelpers
import java.net.URI
import org.json4s._
import org.json4s.jackson.JsonMethods._
import loamstream.util.Loggable
import loamstream.loam.intake.dga.Annotation.Download

/**
 * @author clint
 * Feb 11, 2021
 */
final class AnnotationTest extends FunSuite with Loggable {
  test("isUploadable/notUploadable") {
    val downloads = Seq(
      Annotation.Download(
        AssemblyIds.hg19, 
        URI.create("https://www.example.com/files/DFF258PCW/@@download/DFF258PCW.bed.gz"),
        Annotation.Status.Uploading,
        "ca504ff2221f70a3b98802e36205616e"))
    
    val annotation = Annotation(
      annotationType = AnnotationType.CandidateRegulatoryElements,
      annotationId = "DSR249FPB",
      category = AnnotationCategory.CisRegulatoryElements,
      tissueId = Some("UBERON:0002048"),
      tissue = Some("some-tissue"),
      source = Some("cCRE definitions"),
      assay = Some(Seq("ChIP-seq", "DNase-seq")),
      collection = None,
      biosampleId = Some("EFO:0001196"),
      biosampleType = Some("some-biosample-type"),
      biosample = Some("some-biosample"),
      method = Some("ENCODE-cCREs"), 
      portalUsage = Some("facet"),
      harmonizedStates = Some(Map("x" -> "y")),
      downloads = downloads)
      
    assert(annotation.isUploadable === true)
    assert(annotation.notUploadable === false)
    
    val noDownloads = annotation.copy(downloads = Nil)
    
    assert(noDownloads.isUploadable === false)
    assert(noDownloads.notUploadable === true)
    
    {
      //portalUsage is now ignored
      val noUsage = annotation.copy(portalUsage = Some("None"))
      
      assert(noUsage.isUploadable === true)
      assert(noUsage.notUploadable === false)
    }
    
    {
      //portalUsage is now ignored
      val missingUsage = annotation.copy(portalUsage = None)
      
      assert(missingUsage.isUploadable === true)
      assert(missingUsage.notUploadable === false)
    }
  }

  test("toMetadata") {
    val downloads = Seq(
      Annotation.Download(
        AssemblyIds.hg19, 
        URI.create("https://www.example.com/files/DFF258PCW/@@download/DFF258PCW.bed.gz"),
        Annotation.Status.Uploading,
        "ca504ff2221f70a3b98802e36205616e"))
    
    val annotation = Annotation(
      annotationType = AnnotationType.CandidateRegulatoryElements,
      annotationId = "DSR249FPB",
      category = AnnotationCategory.CisRegulatoryElements,
      tissueId = Some("UBERON:0002048"),
      tissue = Some("some-tissue"),
      source = Some("cCRE definitions"),
      assay = Some(Seq("ChIP-seq", "DNase-seq")),
      collection = None,
      biosampleId = Some("EFO:0001196"),
      biosampleType = Some("some-biosample-type"),
      biosample = Some("some-biosample"),
      method = Some("ENCODE-cCREs"), 
      portalUsage = Some("facet"),
      harmonizedStates = Some(Map("x" -> "y")),
      downloads = downloads,
      derivedFrom = Some(JInt(42)))
      
    val expected = Annotation.Metadata(annotationMethod = Some("ENCODE-cCREs"), derivedFrom = JInt(42))
    
    assert(annotation.toMetadata === expected)
  }
  
  test("fromJson - good input") {
    val idsToNames = Map("UBERON:0002048" -> "some-tissue", "EFO:0001196" -> "some-biosample")
    
    val goodJson = parse(AnnotationTest.goodJson)
    
    val annotation = Annotation.fromJson(idsToNames)(goodJson).get
    
    val expectedDownloads = Seq(
      Annotation.Download(
        AssemblyIds.hg19, 
        URI.create("https://www.example.com/files/DFF258PCW/@@download/DFF258PCW.bed.gz"),
        Annotation.Status.Uploading,
        "ca504ff2221f70a3b98802e36205616e"))
    
    val expected = Annotation(
      annotationType = AnnotationType.CandidateRegulatoryElements,
      annotationId = "DSR249FPB",
      category = AnnotationCategory.CisRegulatoryElements,
      tissueId = Some("UBERON:0002048"),
      tissue = Some("some-tissue"),
      source = Some("cCRE definitions"),
      assay = Some(Seq("ChIP-seq", "DNase-seq")),
      collection = Some(Seq("ENCODE", "FoOoO")),
      biosampleId = Some("EFO:0001196"),
      biosampleType = Some("some-biosample-type"),
      biosample = Some("some-biosample"),
      method = Some("ENCODE-cCREs"), 
      portalUsage = Some("facet"),
      harmonizedStates = Some(Map("x" -> "y", "abc" -> "xyz")),
      downloads = expectedDownloads,
      derivedFrom = Some(goodJson))
    
    assert(annotation === expected)
  }
  
  test("fromJson - bad input") {
    import Annotation.fromJson
    
    assert(fromJson(Map.empty)(parse("{}")).isFailure)
    
    assert(fromJson(Map.empty)(parse("""{"lalala":"asdf"}""")).isFailure)
  }
  
  test("isValidDownload - happy path") {
    val someAnnotationId = "asdasdasd"
    
    for {
      status <- Seq(Annotation.Status.Released, Annotation.Status.Uploading)
      extension <- Seq("bed", "bed.gz")
      assemblyId <- Seq(AssemblyIds.hg19, "GRCh37")
    } {
      val d = Download(assemblyId, URI.create(s"http://example.com/foo.${extension}"), status, md5Sum = "asdf")
      
      assert(Annotation.isValidDownload(someAnnotationId)(d) === true)
    }
  }
   
  test("isValidDownload - invalid cases") {
    val someAnnotationId = "asdasdasd"
    
    val invalidStatus = Annotation.Status.Other
    val invalidExtension = "txt"
    val invalidAssemblyId = "GRCh38"
    
    val d = Download(AssemblyIds.hg19, URI.create(s"http://example.com/foo.bed"), Annotation.Status.Released, md5Sum = "asdf")
    
    def isValidDownload(d: Download) = Annotation.isValidDownload(someAnnotationId)(d)
    
    assert(isValidDownload(d) === true)
      
    val invalidateAssemblyId: Download => Download = _.copy(assemblyId = invalidAssemblyId)
    val invalidateExtension: Download => Download = _.copy(
        url = URI.create(s"http://example.com/foo.${invalidExtension}"),
        file = TestHelpers.path(s"foo.${invalidExtension}"))
    val invalidateStatus: Download => Download = _.copy(status = invalidStatus)
    
    val transforms: Seq[Download => Download] = Seq(invalidateAssemblyId, invalidateExtension, invalidateStatus)
    
    transforms.foreach { t =>
      assert(isValidDownload(t(d)) === false)
    }
    
    transforms.combinations(2).foreach { case Seq(t0, t1) =>
      assert(isValidDownload(t1(t0(d))) === false)
    }
    
    transforms.permutations.foreach { case Seq(t0, t1, t2) =>
      assert(isValidDownload(t2(t1(t0(d)))) === false)
    }
    
    /**
     * //only keep files of the right format, assembly and have been released
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
     */
  }
  
  test("Download.fromJson") {
    import Annotation.Download
    import Annotation.Status
    
    val json = parse("""{
            "files_href": "http://example.com/0.bed",
            "files_assembly": "GRCh37",
            "files_status": "released",
            "files_md5sum": "m0"
          }""")
          
    val download = Annotation.Download.fromJson(json).get
    
    val expected = Annotation.Download("GRCh37", URI.create("http://example.com/0.bed"), Status.Released, "m0")
    
    assert(download === expected)
  }

  test("Status") {
    import Annotation.Status._
    
    assert(fromString("released") === Released)
    assert(fromString("uploading") === Uploading)
    assert(fromString("asdsad") === Other)
    
    assert(Released.countsAsReleased === true)
    assert(Uploading.countsAsReleased === true)
    assert(Other.countsAsReleased === false)
  }

  test("File.isBed/notBed") {
    import TestHelpers.path
    
    val bed = path("/la/la/la/foo/some.bed")
    val bedGz = path("/la/la/la/foo/some.bed.gz")
    val notABed = path("foo/hello.txt")
    
    import Annotation.File.{isBed,notBed}
    
    assert(isBed(bed) === true)
    assert(isBed(bedGz) === true)
    assert(isBed(notABed) === false)
    
    assert(notBed(bed) === false)
    assert(notBed(bedGz) === false)
    assert(notBed(notABed) === true)
  }
  
  test("Metadata - static fields") {
    val m = Annotation.Metadata(None, JInt(42))
    
    assert(m.vendor === "DGA")
    assert(m.version === "1.0")
  }
  
  test("{Metadata,Download}.toJson") {
    import Annotation.Download
    import Annotation.Metadata
    import Annotation.Status
    
    val downloads = Seq(
        Download("a0", URI.create("http://example.com/0.bed"), Status.Released, "m0"),
        Download("a1", URI.create("http://example.com/1.bed.gz"), Status.Uploading, "m1"))

    val m = Metadata(Some("foo-method"), derivedFrom = JInt(42))
    
    val expected = compact(render(parse("""{
        "vendor": "DGA",
        "version": "1.0",
        "method": "foo-method",
        "derivedFrom": 42
      }""")))
        
    assert(compact(render(m.toJson)) === expected)
  }
}

object AnnotationTest {
  val goodJson: String = """
{
            "dbxrefs": [
                "ENCODE:ENCSR599FOY"
            ],
            "annotation_source": "cCRE definitions",
            "collection_tags": [
                "ENCODE",
                "FoOoO"
            ],
            "publications": [
                "doi:10.1038/s41586-020-2493-4",
                "PMID:32728249",
                "PMCID:PMC7410828"
            ],
            "annotation_method": "ENCODE-cCREs",
            "portal_tissue_id": "UBERON:0002048",
            "annotation_category": "cis-regulatory elements",
            "underlying_assay": [
                "ChIP-seq",
                "DNase-seq"
            ],
            "documents": {},
            "biosample_term_id": "EFO:0001196",
            "annotation_id": "DSR249FPB",
            "portal_tissue": [
                "lung"
            ],
            "annotation_type": "candidate regulatory elements",
            "file_download": {
                "DFF258PCW": [
                    {
                        "output_type": "candidate regulatory elements",
                        "files_href": "https://www.example.com/files/DFF258PCW/@@download/DFF258PCW.bed.gz",
                        "files_assembly": "hg19",
                        "files_md5sum": "ca504ff2221f70a3b98802e36205616e",
                        "files_lab": "AMP-T2D consortium",
                        "files_date_created": "2020-09-19T18:19:57.270962+00:00",
                        "files_status": "uploading"
                    }
                ]
            },
            "project": "AMP",
            "biosample_term_name": "IMR-90",
            "portal_usage": "facet",
            "harmonized_states": {"x": "y", "abc": "xyz"},
            "dataset_status": "proposed",
            "biosample_type": "some-biosample-type"
        }
""".trim
}
