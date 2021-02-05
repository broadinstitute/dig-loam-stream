package loamstream.util

import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.intake.dga.DgaSyntax
import loamstream.loam.intake.AwsRowSink
import loamstream.loam.intake.dga.Tissue

/**
 * @author clint
 * Dec 8, 2020
 */
final class DgaTissueUploadTest extends AwsFunSuite with IntakeSyntax with DgaSyntax with Loggable {
  testWithPseudoDir(s"${getClass.getSimpleName}-Read_from_DGA_upload_to_S3") { testDir =>
    val (versionSource, tissueSource) = Dga.Tissues.versionAndTissueSource()
    
    assert(versionSource.records.next() === "3")
    
    val uuid = java.util.UUID.randomUUID.toString
    
    val awsClient = new AwsClient.Default(aws)
    
    val sink = new AwsRowSink(
        topic = "some-topic",
        name = "some-name",
        batchSize = 10000, //10k
        awsClient = awsClient,
        baseDir = Some(testDir),
        // :(
        yes = true,
        uuid = uuid)
    
    val count: Fold[Tissue, Int, Int] = Fold.count
    val upload: Fold[Tissue, Unit, Unit] = Fold.foreach(sink.accept)
    
    val m = count |+| upload
    
    val filtered = tissueSource.filter(_.isValid)
    
    TimeUtils.time("Uploading tissues", info(_)) {
      info("Uploading tissues...")
      
      val (n, _) = try { m.process(filtered.records) } finally { sink.close() }
      
      info(s"Uploaded $n valid tissues")
    }
    
    val uploaded = awsClient.list(s"${testDir}")
    
    val expectedKeys = Set(
        s"${testDir}/some-topic/some-name/part-00000-${uuid}.json",
        s"${testDir}/some-topic/some-name/part-00001-${uuid}.json",
        s"${testDir}/some-topic/some-name/part-00002-${uuid}.json")
    
    assert(uploaded.toSet == expectedKeys)
  }
}
