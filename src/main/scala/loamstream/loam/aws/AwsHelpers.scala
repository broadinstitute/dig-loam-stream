package loamstream.loam.aws

import java.net.URI

import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.Cluster

import loamstream.aws.AwsJobDesc
import loamstream.loam.LoamScriptContext
import loamstream.loam.ToolSyntaxCompanion
import loamstream.model.PathStore
import loamstream.model.Store
import loamstream.model.UriStore
import loamstream.model.execute.AwsSettings
import loamstream.model.execute.AwsClusterSettings
import loamstream.aws.AwsClient

/**
 * @author clint
 * Oct 17, 2019
 */
trait AwsHelpers {
  import AwsHelpers.addToGraph
  
  def awsUriOf(key: String)(implicit awsClient: AwsClient): URI = awsClient.uriOf(key)
  
  def awsCopy(
      srcs: Iterable[Store],
      dests: Iterable[Store])(implicit context: LoamScriptContext): Iterable[AwsTool] = {

    srcs.zip(dests).map { case (src, dest) => awsCopy(src, dest) }
  }

  def awsCopy(src: Store, dest: Store)(implicit context: LoamScriptContext): AwsTool = requireAwsConfig {
    import AwsTool.{ DownloadFromS3AwsTool, UploadToS3AwsTool }
    
    val tool = (src, dest) match {
      case (PathStore(_, srcPath), UriStore(_, destUri)) => UploadToS3AwsTool(srcPath, destUri)(context)
      case (UriStore(_, srcUri), PathStore(_, destPath)) => DownloadFromS3AwsTool(srcUri, destPath)(context)
      //TODO
      case (s, d) => sys.error(s"Unexpected src and dest: $s AND $d")
    }
    
    addToGraph(tool).in(src).out(dest)
  }
  
  def awsPySpark(
      scriptUri: URI, 
      args: String*)(implicit context: LoamScriptContext): AwsTool.PySparkAwsTool = requireAwsEnv {

    val (cluster, _) = awsClusterConfig(context)
    
    addToGraph(AwsTool.PySparkAwsTool(cluster, scriptUri, args))
  }
  
  def awsScript(
      scriptUri: URI, 
      args: String*)(implicit context: LoamScriptContext): AwsTool.RunScriptAwsTool = requireAwsEnv {
    
    val (cluster, _) = awsClusterConfig(context)
    
    addToGraph(AwsTool.RunScriptAwsTool(cluster, scriptUri, args))
  }
  
  def awsScript(script: URI, args: String*): JobStep.Script = JobStep.Script(script, args: _*)
  
  def awsPySparkScript(script: URI, args: String*): JobStep.Script = JobStep.Script(script, args: _*)
  
  def awsClusterJobs(awsJobs: AwsJobDesc*)(implicit context: LoamScriptContext): AwsTool = requireAwsEnv {
    val (cluster, maxClusters) = awsClusterConfig(context)
    
    addToGraph(AwsTool.PooledAwsTool(cluster, maxClusters, awsJobs))
  }
  
  private def awsClusterConfig(implicit context: LoamScriptContext): (Cluster, Int) = context.settings match {
    case AwsClusterSettings(cluster, maxClusters) => (cluster, maxClusters)
    case settings => sys.error(s"${classOf[AwsClusterSettings].getSimpleName} expected, but got ${settings}")
  }
  
  private def requireAwsEnv[A](body: => A)(implicit context: LoamScriptContext): A = requireAwsConfig {
    require(context.settings.isAws, s"Expected to be invoked from within an awsWith(...) { } block")
    
    body
  }
  
  private def requireAwsConfig[A](body: => A)(implicit context: LoamScriptContext): A = {
    require(context.config.awsConfig.isDefined, s"Expected AWS support to be configured in loamstream.conf")
    
    body
  }
}

object AwsHelpers extends ToolSyntaxCompanion[AwsTool] 
