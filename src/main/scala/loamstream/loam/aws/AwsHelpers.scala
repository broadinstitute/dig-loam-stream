package loamstream.loam.aws

import loamstream.loam.LoamScriptContext
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.PathStore
import loamstream.model.UriStore
import java.net.URI
import loamstream.model.execute.AwsSettings
import org.broadinstitute.dig.aws.JobStep
import org.broadinstitute.dig.aws.emr.Cluster
import loamstream.aws.AwsJobDesc
import loamstream.loam.ToolSyntaxCompanion

/**
 * @author clint
 * Oct 17, 2019
 */
trait AwsHelpers {
  import AwsHelpers.addToGraph
  
  def awsCopy(
      srcs: Iterable[Store],
      dests: Iterable[Store])(implicit context: LoamScriptContext): Iterable[AwsTool] = {

    for((src, dest) <- srcs.zip(dests)) yield {
      awsCopy(src, dest)
    }
  }

  def awsCopy(src: Store, dest: Store)(implicit context: LoamScriptContext): AwsTool = {
    import AwsTool.{UploadToS3AwsTool, DownloadFromS3AwsTool}
    
    val tool = (src, dest) match {
      case (PathStore(_, srcPath), UriStore(_, destUri)) => UploadToS3AwsTool(srcPath, destUri)(context)
      case (UriStore(_, srcUri), PathStore(_, destPath)) => DownloadFromS3AwsTool(srcUri, destPath)(context)
      //TODO
      case (s, d) => sys.error(s"Unexpected src and dest: $s AND $d")
    }
    
    addToGraph(tool).in(src).out(dest)
  }
  
  private def awsClusterConfig(implicit context: LoamScriptContext): (Cluster, Int) = context.settings match {
    case AwsSettings(cluster, maxClusters) => (cluster, maxClusters)
    case settings => sys.error(s"AWS settings expected, but got ${settings}")
  }
  
  def awsPySpark(scriptUri: URI, args: String*)(implicit context: LoamScriptContext): AwsTool.PySparkAwsTool = {
    val (cluster, _) = awsClusterConfig(context)
    
    addToGraph(AwsTool.PySparkAwsTool(cluster, scriptUri, args))
  }
  
  def awsScript(scriptUri: URI, args: String*)(implicit context: LoamScriptContext): AwsTool.RunScriptAwsTool = {
    val (cluster, _) = awsClusterConfig(context)
    
    addToGraph(AwsTool.RunScriptAwsTool(cluster, scriptUri, args))
  }
  
  def awsScript(script: URI, args: String*): JobStep.Script = JobStep.Script(script, args: _*)
  
  def awsPySparkScript(script: URI, args: String*): JobStep.Script = JobStep.Script(script, args: _*)
  
  def awsClusterJobs(awsJobs: AwsJobDesc*)(implicit context: LoamScriptContext): AwsTool = {
    val (cluster, maxClusters) = awsClusterConfig(context)
    
    addToGraph(AwsTool.PooledAwsTool(cluster, maxClusters, awsJobs))
  }
}

object AwsHelpers extends ToolSyntaxCompanion[AwsTool] 
