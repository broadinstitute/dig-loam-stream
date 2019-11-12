package loamstream.aws

import java.net.URI
import java.nio.file.Path
import java.nio.file.Files.exists
import java.time.Instant

import org.broadinstitute.dig.aws.emr.Cluster
import org.scalatest.FunSuite

import loamstream.util.Hash
import loamstream.util.Hashes
import loamstream.util.HashType
import loamstream.util.Files
import loamstream.util.ValueBox
import loamstream.aws.AwsChunkRunnerTest.MockAwsClient
import loamstream.TestHelpers
import loamstream.util.Observables
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.aws.AwsJob
import loamstream.model.execute.AwsSettings
import loamstream.util.Paths

/**
 * @author clint
 * Oct 30, 2019
 */
final class AwsChunkRunnerTest extends FunSuite {
  import AwsChunkRunnerTest.MockAwsClient
  import TestHelpers.DummyJobOracle
  import TestHelpers.neverRestart
  import TestHelpers.waitFor
  import Observables.Implicits._
  
  test("Empty input") {
    val awsChunkRunner = new AwsChunkRunner(new MockAwsClient)
    
    val obs = awsChunkRunner.run(Set.empty, DummyJobOracle, neverRestart)
    
    val result = waitFor(obs.lastAsFuture)
    
    assert(result === Map.empty)
  }
  
  test("Guards") {
    val awsChunkRunner = new AwsChunkRunner(new MockAwsClient)
    
    //Local job
    val mockJob = MockJob(JobStatus.Succeeded)
    
    intercept[Exception] {
      awsChunkRunner.run(Set(mockJob), DummyJobOracle, neverRestart)
    }
    
    val awsJob = AwsJob(_ => ???, AwsSettings)
    
    intercept[Exception] {
      awsChunkRunner.run(Set(awsJob, mockJob), DummyJobOracle, neverRestart)
    }
    
    intercept[Exception] {
      awsChunkRunner.run(Set(awsJob, mockJob, awsJob), DummyJobOracle, neverRestart)
    }
  }
  
  test("run") {
    val awsClient = new MockAwsClient
    
    val awsChunkRunner = new AwsChunkRunner(awsClient)
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      import Paths.Implicits._
      
      val src = workDir / "foo.txt"
      val uri = URI.create("s3://foo/bar/baz")
      val dest = workDir / "bar.txt" 
      
      Files.writeTo(src)("ASDF")
      
      assert(exists(src) === true)
      assert(awsClient.exists(uri) === false)
      assert(exists(dest) === false)
      
      val toS3 = AwsJob(_.copy(src, uri), AwsSettings)
      val fromS3 = AwsJob(_.copy(uri, dest), AwsSettings)
      
      val obs0 = awsChunkRunner.run(Set(toS3), DummyJobOracle, neverRestart)
      
      val result0 = waitFor(obs0.firstAsFuture)
      
      assert(exists(src) === true)
      assert(awsClient.exists(uri) === true)
      assert(exists(dest) === false)
      
      val obs1 = awsChunkRunner.run(Set(fromS3), DummyJobOracle, neverRestart)
      
      val result1 = waitFor(obs1.firstAsFuture)
      
      val result = result0 ++ result1
      
      assert(result.values.forall(_.jobStatus.isSuccess))
      
      assert(exists(src) === true)
      assert(awsClient.exists(uri) === true)
      assert(exists(dest) === true)
      
      assert(Files.readFrom(src) === Files.readFrom(dest))
    }
  }
}

object AwsChunkRunnerTest {
  final case class MockS3Entry(var value: String, var lastModified: Instant, var hash: String)
  
  final class MockAwsClient extends AwsClient {
    
    private[AwsChunkRunnerTest] val s3: ValueBox[Map[String, MockS3Entry]] = ValueBox(Map.empty)
    
    private def toKey(u: URI): String = u.toString
    
    override def copy(src: Path, dest: URI): Unit = {
      import java.nio.file.{ Files => JFiles }
      
      require(JFiles.exists(src))
          
      val key = toKey(dest)
      
      val fileContents = Files.readFrom(src)
      val lastModified = Instant.now
      val hashValue = Hashes.digest(HashType.Md5)(Iterator(JFiles.readAllBytes(src))).valueAsBase64String
      
      s3.mutate { s3Map =>
        s3Map + (key -> MockS3Entry(fileContents, lastModified, hashValue))
      }
    }
  
    override def copy(src: URI, dest: Path): Unit = s3.foreach { s3Map =>
      require(exists(src))
      
      Files.writeTo(dest)(s3Map(toKey(src)).value)
    }
  
    override def exists(uri: URI): Boolean = s3.get(_.contains(toKey(uri)))
  
    override def hash(uri: URI): Option[Hash] = s3.get(_.get(toKey(uri))).map(s3e => Hash(s3e.hash, HashType.Md5))
  
    override def lastModified(uri: URI): Option[Instant] = s3.get(_.get(toKey(uri))).map(_.lastModified)
  
    override def runPySparkJob(cluster: Cluster, pySparkScriptUri: URI, scriptArgs: Seq[String]): Unit = ???
  
    override def runScript(cluster: Cluster, scriptUri: URI, scriptArgs: Seq[String]): Unit = ???
  
    override def runOnClusterPool(cluster: Cluster, maxClusters: Int, awsJobs: Seq[AwsJobDesc]): Unit = ???
  }
}
