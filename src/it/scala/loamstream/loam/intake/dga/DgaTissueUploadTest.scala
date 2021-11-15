package loamstream.util

import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.dga.DgaSyntax
import loamstream.loam.intake.AwsRowSink
import loamstream.loam.intake.dga.Tissue
import org.json4s._

/**
 * @author clint
 * Dec 8, 2020
 * 
 * NOTE: disabled (made abstract) since the DGA REST API changed since this was written.
 * TODO: Evaluate if this is worth salvaging, or if a different approach to testing the DGA support would be better.
 * A "live" integration test like this shouldn't run on every commit at a minimum, since the DGA REST API is only 
 * brought up at specific ingestion times and is otherwise unavailable.
 */

abstract class DgaTissueUploadTest extends AwsFunSuite with IntakeSyntax with DgaSyntax with Loggable {
  testWithPseudoDir(s"${getClass.getSimpleName}-Read_from_DGA_upload_to_S3") { testDir =>
    val (versionSource, tissueSource) = Dga.Tissues.versionAndTissueSource()
    
    assert(versionSource.records.next() === "3")
    
    val uuid = java.util.UUID.randomUUID.toString
    
    val sink = new AwsRowSink(
        topic = "some-topic",
        dataset = "some-name",
        techType = None,
        phenotype = None,
        batchSize = 10000, //10k
        s3Client = s3Client,
        baseDir = Some(testDir),
        uuid = uuid,
        metadata = JObject("foo" -> JInt(42)))
    
    val count: Fold[Tissue, Int, Int] = Fold.count
    val upload: Fold[Tissue, Unit, Unit] = Fold.foreach(sink.accept)
    
    val m = count |+| upload
    
    val filtered = tissueSource.filter(_.isValid)
    
    TimeUtils.time("Uploading tissues", info(_)) {
      info("Uploading tissues...")
      
      val (n, _) = try { m.process(filtered.records) } finally { sink.close() }
      
      info(s"Uploaded $n valid tissues")
    }
    
    val uploaded = s3Client.list(s"${testDir}")
    
    val expectedKeys = Set(
        s"${testDir}/some-topic/some-name/part-00000-${uuid}.json",
        s"${testDir}/some-topic/some-name/part-00001-${uuid}.json",
        s"${testDir}/some-topic/some-name/part-00002-${uuid}.json")
    
    assert(uploaded.toSet == expectedKeys)
  }
}
