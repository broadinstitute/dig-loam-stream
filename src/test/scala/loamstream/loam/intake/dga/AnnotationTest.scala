package loamstream.loam.intake.dga

import org.scalatest.FunSuite
import loamstream.TestHelpers
import java.net.URI
import org.json4s._
import org.json4s.jackson.JsonMethods._
import loamstream.util.Loggable

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
      assembly = AssemblyIds.hg19,
      annotationType = "candidate_regulatory_regions",
      annotationId = "DSR249FPB",
      category = Some("cis-regulatory elements"),
      tissueId = Some("UBERON:0002048"),
      tissue = Some("some-tissue"),
      source = Some("cCRE definitions"),
      assay = Some(Seq("ChIP-seq", "DNase-seq")),
      collection = None,
      biosampleId = "EFO:0001196",
      biosampleType = "some-biosample-type",
      biosample = Some("some-biosample"),
      method = Some("ENCODE-cCREs"), 
      portalUsage = "facet",
      downloads = downloads)
      
    assert(annotation.isUploadable === true)
    assert(annotation.notUploadable === false)
    
    val noDownloads = annotation.copy(downloads = Nil)
    
    assert(noDownloads.isUploadable === false)
    assert(noDownloads.notUploadable === true)
    
    val noUsage = annotation.copy(portalUsage = "None")
    
    assert(noUsage.isUploadable === false)
    assert(noUsage.notUploadable === true)
  }

  test("portalUsageIsNone") {
    val a = Annotation(
      assembly = AssemblyIds.hg19,
      annotationType = "candidate_regulatory_regions",
      annotationId = "DSR249FPB",
      category = Some("cis-regulatory elements"),
      tissueId = Some("UBERON:0002048"),
      tissue = Some("some-tissue"),
      source = Some("cCRE definitions"),
      assay = Some(Seq("ChIP-seq", "DNase-seq")),
      collection = None,
      biosampleId = "EFO:0001196",
      biosampleType = "some-biosample-type",
      biosample = Some("some-biosample"),
      method = Some("ENCODE-cCREs"), 
      portalUsage = "facet",
      downloads = Nil)
      
    assert(a.portalUsageIsNone === false)
    assert(a.copy(portalUsage = "none").portalUsageIsNone === false)
    assert(a.copy(portalUsage = "None").portalUsageIsNone === true)
  }
  
  test("toMetadata") {
    val downloads = Seq(
      Annotation.Download(
        AssemblyIds.hg19, 
        URI.create("https://www.example.com/files/DFF258PCW/@@download/DFF258PCW.bed.gz"),
        Annotation.Status.Uploading,
        "ca504ff2221f70a3b98802e36205616e"))
    
    val annotation = Annotation(
      assembly = AssemblyIds.hg19,
      annotationType = "candidate_regulatory_regions",
      annotationId = "DSR249FPB",
      category = Some("cis-regulatory elements"),
      tissueId = Some("UBERON:0002048"),
      tissue = Some("some-tissue"),
      source = Some("cCRE definitions"),
      assay = Some(Seq("ChIP-seq", "DNase-seq")),
      collection = None,
      biosampleId = "EFO:0001196",
      biosampleType = "some-biosample-type",
      biosample = Some("some-biosample"),
      method = Some("ENCODE-cCREs"), 
      portalUsage = "facet",
      downloads = downloads)
      
    val expected = Annotation.Metadata(downloads, Some("ENCODE-cCREs"))
    
    assert(annotation.toMetadata === expected)
  }
  
  test("fromJson - good input") {
    val idsToNames = Map("UBERON:0002048" -> "some-tissue", "EFO:0001196" -> "some-biosample")
    
    val annotation = Annotation.fromJson(AssemblyIds.hg19, idsToNames)(parse(AnnotationTest.goodJson)).get
    
    val expectedDownloads = Seq(
      Annotation.Download(
        AssemblyIds.hg19, 
        URI.create("https://www.example.com/files/DFF258PCW/@@download/DFF258PCW.bed.gz"),
        Annotation.Status.Uploading,
        "ca504ff2221f70a3b98802e36205616e"))
    
    val expected = Annotation(
      assembly = AssemblyIds.hg19,
      annotationType = "candidate regulatory regions",//Spaces will be dealt with by BedRowExpr
      annotationId = "DSR249FPB",
      category = Some("cis-regulatory elements"),
      tissueId = Some("UBERON:0002048"),
      tissue = Some("some-tissue"),
      source = Some("cCRE definitions"),
      assay = Some(Seq("ChIP-seq", "DNase-seq")),
      collection = Some(Seq("ENCODE", "FoOoO")),
      biosampleId = "EFO:0001196",
      biosampleType = "some-biosample-type",
      biosample = Some("some-biosample"),
      method = Some("ENCODE-cCREs"), 
      portalUsage = "facet",
      downloads = expectedDownloads)
    
    assert(annotation === expected)
  }
  
  test("fromJson - bad input") {
    import Annotation.fromJson
    
    assert(fromJson("a0", Map.empty)(parse("{}")).isFailure)
    
    assert(fromJson("a0", Map.empty)(parse("""{"lalala":"asdf"}""")).isFailure)
  }
  
  ignore("isValidDownload") {
    fail("TODO")
  }
  
  test("Download.fromJson") {
    import Annotation.Download
    import Annotation.Status
    
    val json = parse("""{
            "files_href": "http://example.com/0.bed",
            "files_status": "released",
            "files_md5sum": "m0"
          }""")
          
    val download = Annotation.Download.fromJson("a0")(json).get
    
    val expected = Annotation.Download("a0", URI.create("http://example.com/0.bed"), Status.Released, "m0")
    
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
    val m = Annotation.Metadata(Nil, None)
    
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

    val m = Metadata(downloads, Some("foo-method"))
    
    val expected = compact(render(parse("""{
        "sources": [
          {
            "assemblyId": "a0",
            "url": "http://example.com/0.bed",
            "file": "/0.bed",
            "status": "released",
            "md5Sum": "m0"
          },
          {
            "assemblyId": "a1",
            "url": "http://example.com/1.bed.gz",
            "file": "/1.bed.gz",
            "status": "uploading",
            "md5Sum": "m1"
          }
        ],
        "method": "foo-method"
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
            "annotation_type": "candidate regulatory regions",
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
            "harmonized_states": null,
            "dataset_status": "proposed",
            "biosample_type": "some-biosample-type"
        }
""".trim
}
