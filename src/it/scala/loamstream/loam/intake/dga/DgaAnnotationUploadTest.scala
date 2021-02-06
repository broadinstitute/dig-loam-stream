package loamstream.util

import loamstream.loam.intake.dga.Annotation
import loamstream.loam.intake.dga.AnnotationsSupport
import loamstream.loam.intake.dga.AssemblyIds
import loamstream.loam.intake.dga.DgaSyntax
import loamstream.util.HttpClient.Auth
import scala.util.Success
import loamstream.loam.intake.ToFileLogContext
import java.nio.file.{Paths => JPaths}

/**
 * @author clint
 * Dec 8, 2020
 */
final class DgaAnnotationsUploadTest extends AwsFunSuite with DgaSyntax with Loggable {
  testWithPseudoDir(s"${getClass.getSimpleName}-Read_from_DGA_upload_to_S3", nukeTestDirOnSuccess = false) { testDir =>
    //TODO
    val auth: Auth = ???
    
    val awsClient = new AwsClient.Default(aws)
    
    val annotations = Dga.Annotations.downloadAnnotations(AssemblyIds.hg19)
    
    //TODO
    val logCtx = new ToFileLogContext(JPaths.get("./bad-annotations"))
    
    val uploadableAnnotations = annotations.
      filter(Dga.Annotations.Predicates.isUploadable(logCtx).liftToTry).
      filter(Dga.Annotations.Predicates.hasAnnotationTypeWeCareAbout(logCtx).liftToTry).
      collect { case Success(a) => a } //TODO
      
    //TODO: FIXME: Process more 
    val firstN = uploadableAnnotations.take(10).records.foreach {
      Dga.Annotations.uploadAnnotatedDataset(auth, awsClient, yes = true)
    }
  }
}
