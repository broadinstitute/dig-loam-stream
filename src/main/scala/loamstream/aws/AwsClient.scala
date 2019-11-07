package loamstream.aws

import java.net.URI
import java.nio.file.Path
import org.broadinstitute.dig.aws.AWS
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.Cluster
import java.time.Instant
import loamstream.util.Hash
import loamstream.util.HashType

/**
 * @author clint
 * Oct 17, 2019
 */
trait AwsClient {
  def uriOf(key: String): URI
  
  def copy(src: Path, dest: URI): Unit
  
  def copy(src: URI, dest: Path): Unit
  
  def exists(uri: URI): Boolean
  
  def hash(uri: URI): Option[Hash]
  
  def lastModified(uri: URI): Option[Instant]
  
  def runPySparkJob(cluster: Cluster, pySparkScriptUri: URI, scriptArgs: Seq[String]): Unit
  
  def runScript(cluster: Cluster, scriptUri: URI, scriptArgs: Seq[String]): Unit
  
  def runOnClusterPool(cluster: Cluster, maxClusters: Int, awsJobs: Seq[AwsJobDesc]): Unit
}

object AwsClient {
  def apply(aws: AWS): AwsClient = Default(aws)
  
  final case class Default(aws: AWS) extends AwsClient {
    override def uriOf(key: String): URI = aws.uriOf(key)
    
    override def copy(src: Path, dest: URI): Unit = {
      require(AWS.bucketOf(dest) == aws.bucket)
      
      aws.put(AWS.keyOf(dest), src).unsafeRunSync()
    }
  
    override def copy(src: URI, dest: Path): Unit = {
      aws.download(AWS.keyOf(src), dest).unsafeRunSync()
    }
    
    override def exists(uri: URI): Boolean = aws.exists(AWS.keyOf(uri)).unsafeRunSync()
    
    override def hash(uri: URI): Option[Hash] = {
      val eTagOpt = aws.eTagOf(AWS.keyOf(uri)).unsafeRunSync()
      
      //NB: AWS ETags might or might not be MD5 hashes, based on a variety of factors (upload method, crypto or no
      //crypto, etc, etc) but in our case we can be reasonably sure they're MD5s.  In any case, the values from AWS
      //just need to be consistent for the same file over time - which they are - and the exact HashType doesn't
      //matter.
      eTagOpt.map(eTag => Hash(eTag, HashType.Md5))
    }
  
    override def lastModified(uri: URI): Option[Instant] = aws.lastModifiedTimeOf(AWS.keyOf(uri)).unsafeRunSync()
    
    override def runPySparkJob(cluster: Cluster, pySparkScriptUri: URI, scriptArgs: Seq[String]): Unit = {
      runSingleStep(cluster, JobStep.PySpark(pySparkScriptUri, scriptArgs: _*))
    }
    
    override def runScript(cluster: Cluster, scriptUri: URI, scriptArgs: Seq[String]): Unit = {
      runSingleStep(cluster, JobStep.Script(scriptUri, scriptArgs: _*))
    }
    
    override def runOnClusterPool(cluster: Cluster, maxClusters: Int, awsJobs: Seq[AwsJobDesc]): Unit = {
      val jobIOs = aws.clusterJobs(cluster, awsJobs, maxClusters).toList
      
      val io = aws.waitForJobs(jobIOs)
      
      io.unsafeRunSync()
    }
    
    private def runSingleStep(cluster: Cluster, step: JobStep): Unit = {
      val io = for {
        job <- aws.runJob(cluster, step)
        _ <- aws.waitForJob(job)
      } yield ()
        
      io.unsafeRunSync()
    }
  }
}
