package loamstream.loam.aws

import java.net.URI
import java.nio.file.Path

import org.broadinstitute.dig.aws.AWS
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.Cluster

import loamstream.aws.AwsJobDesc

/**
 * @author clint
 * Oct 17, 2019
 */
trait AwsApi {
  def copy(src: Path, dest: URI): Unit
  
  def copy(src: URI, dest: Path): Unit
  
  def exists(uri: URI): Boolean
  
  def runPySparkJob(cluster: Cluster, pySparkScriptUri: URI, scriptArgs: Seq[String]): Unit
  
  def runScript(cluster: Cluster, scriptUri: URI, scriptArgs: Seq[String]): Unit
  
  def runOnClusterPool(cluster: Cluster, maxClusters: Int, awsJobs: Seq[AwsJobDesc]): Unit
}

object AwsApi {
  def apply(aws: AWS): AwsApi = Default(aws)
  
  final case class Default(aws: AWS) extends AwsApi {
    override def copy(src: Path, dest: URI): Unit = {
      require(AWS.bucketOf(dest) == aws.bucket)

      println(s"%%%%%%%%% copying '$src' ==> '$dest'")
      
      aws.put(AWS.keyOf(dest), src).unsafeRunSync()
    }
  
    override def copy(src: URI, dest: Path): Unit = {
      println(s"%%%%%%%%% copying '$src' ==> '$dest'")
      
      aws.download(AWS.keyOf(src), dest).unsafeRunSync()
    }
    
    override def exists(uri: URI): Boolean = aws.exists(AWS.keyOf(uri)).unsafeRunSync()
    
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
