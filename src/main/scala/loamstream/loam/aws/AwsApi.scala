package loamstream.loam.aws

import java.net.URI
import java.nio.file.Path
import org.broadinstitute.dig.aws.AWS
import cats.Id
import rx.lang.scala.Observable
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
  def apply(aws: AWS[Id]): AwsApi = Default(aws)
  
  final case class Default(aws: AWS[Id]) extends AwsApi {
    override def copy(src: Path, dest: URI): Unit = {
      require(aws.bucketOf(dest) == aws.bucket)

      println(s"%%%%%%%%% copying '$src' ==> '$dest'")
      
      aws.put(aws.keyOf(dest), src)
    }
  
    override def copy(src: URI, dest: Path): Unit = {
      println(s"%%%%%%%%% copying '$src' ==> '$dest'")
      
      aws.download(aws.keyOf(src), dest)
    }
    
    override def exists(uri: URI): Boolean = aws.exists(aws.keyOf(uri))
    
    override def runPySparkJob(cluster: Cluster, pySparkScriptUri: URI, scriptArgs: Seq[String]): Unit = {
      runSingleStep(cluster, JobStep.PySpark(pySparkScriptUri, scriptArgs: _*))
    }
    
    override def runScript(cluster: Cluster, scriptUri: URI, scriptArgs: Seq[String]): Unit = {
      runSingleStep(cluster, JobStep.Script(scriptUri, scriptArgs: _*))
    }
    
    override def runOnClusterPool(cluster: Cluster, maxClusters: Int, awsJobs: Seq[AwsJobDesc]): Unit = {
      val jobs = aws.clusterJobs(cluster, awsJobs, maxClusters)
      
      aws.waitForJobs(jobs)
    }
    
    private def runSingleStep(cluster: Cluster, step: JobStep): Unit = {
      val job = aws.runJob(cluster, step) 
        
      aws.waitForJob(job)
    }
  }
}
