package loamstream.util

import java.io.File
import java.nio.file.{ Paths => JPaths }

import scala.util.Success

import loamstream.loam.intake.ToFileLogContext
import loamstream.loam.intake.dga.AssemblyIds
import loamstream.loam.intake.dga.DgaSyntax
import loamstream.util.HttpClient.Auth

/**
 * @author clint
 * Dec 8, 2020
 */
final class DgaAnnotationsUploadTest extends AwsFunSuite with DgaSyntax with Loggable {
  testWithPseudoDir(s"${getClass.getSimpleName}-Read_from_DGA_upload_to_S3", nukeTestDirOnSuccess = false) { testDir =>
    //TODO
    val auth: Auth = ???
    
    val awsClient = new AwsClient.Default(aws)
    
    val annotationJsonFile = "/home/clint/workspace/dig-aggregator-intake/dga/annotations/raw-json-output-2021-02-22.json"
  
    def cannedAnnotationData: String = CanBeClosed.using(scala.io.Source.fromFile(new File(annotationJsonFile))) {
      _.mkString
    }
    
    val tissueJsonFile = "/home/clint/workspace/dig-aggregator-intake/dga/tissues/ontology/raw-dga-output.json"
    
    def cannedTissueData: String = CanBeClosed.using(scala.io.Source.fromFile(new File(annotationJsonFile))) {
      _.mkString
    }
    
    val (_, tissueSource) = Dga.Tissues.versionAndTissueSource(cannedTissueData)
    
    val annotations = Dga.Annotations.downloadAnnotations(
        annotationJsonString = cannedAnnotationData, 
        tissueSource = tissueSource)
    
    //TODO
    val logCtx = new ToFileLogContext(JPaths.get("./bad-annotations"))
    
    val uploadableAnnotations = annotations.
      filter(Dga.Annotations.Predicates.isUploadable(logCtx).liftToTry).
      filter(Dga.Annotations.Predicates.hasAnnotationTypeWeCareAbout(logCtx).liftToTry).
      filter(Dga.Annotations.Predicates.succeeded(logCtx)).
      collect { case Success(a) => a } //TODO
      
    val annotationIdsToUpload: Set[String] = Set(
        "dsr218cnx", 
        "dsr269wmz",
        "dsr429dxe",
        "dsr546lsw",
        "dsr590hep",
        "dsr841rlu",
        "tstsr043890",
        "tstsr538274",
        "tstsr679993",
        "tstsr798156")
    
    //TODO: FIXME: Process more 
    val firstN = uploadableAnnotations.take(10).records.foreach {
      
      Dga.Annotations.uploadAnnotatedDataset(auth, awsClient, logCtx, yes = true)
    }
  }
}
