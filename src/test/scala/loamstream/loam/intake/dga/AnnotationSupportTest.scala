package loamstream.loam.intake.dga

import org.scalatest.FunSuite
import loamstream.util.Loggable
import loamstream.TestHelpers
import loamstream.loam.intake.ToFileLogContext

/**
 * @author clint
 * May 5, 2021
 */
final class AnnotationSupportTest extends FunSuite {
  private def minimalAnnotation(id: String, category: AnnotationCategory) = Annotation(
      annotationType = AnnotationType.AccessibleChromatin,
      annotationId = id,
      category = category,
      tissueId = None,
      tissue = None,
      source = None,
      assay = None,
      collection = None,
      biosampleId = None,
      biosampleType = None,
      biosample = None,
      method = None,
      diseaseTermId = None,
      diseaseTermName = None,
      portalUsage = None, 
      harmonizedStates = None,
      downloads = Nil,
      derivedFrom = Some(org.json4s.JNull))
  
  private object Dsl extends AnnotationsSupport with Loggable with BedSupport with TissueSupport
      
  test("Annotations.UploadOps.datasetName") {
    def doTest(annotationId: String, expectedDatasetName: String): Unit = {
      for {
        category <- AnnotationCategory.values.toSeq
      } {
        val ann = minimalAnnotation(annotationId, category)
        
        val ops = new Dsl.Annotations.UploadOps(
            annotation = ann,
            auth = None,
            awsClient = DummyS3Client,
            logCtx = new ToFileLogContext(TestHelpers.path("/dev/null"))) // :\
        
        assert(ops.datasetName === expectedDatasetName)
      }
    }
    
    doTest("foo", "foo")
    doTest("BaR", "bar")
    doTest("la LA  la", "la_la_la")
  }
  
  test("Annotations.UploadOps.topicName") {
    for {
      id <- Seq("foo", "bar")
      category <- AnnotationCategory.values.toSeq
    } {
      val ann = minimalAnnotation(id, category)
      
      val ops = new Dsl.Annotations.UploadOps(
          annotation = ann,
          auth = None,
          awsClient = DummyS3Client,
          logCtx = new ToFileLogContext(TestHelpers.path("/dev/null"))) // :\
      
      assert(ops.topicName === s"annotated_regions/${ann.category.name}")
    }
  }
}