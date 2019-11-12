package loamstream.loam.aws

import java.net.URI
import java.nio.file.Path

import org.broadinstitute.dig.aws.emr.Cluster

import loamstream.loam.LoamScriptContext
import loamstream.loam.ToolSyntaxCompanion
import loamstream.model.LId
import loamstream.model.Tool
import loamstream.model.Tool.DefaultStores
import loamstream.aws.AwsJobDesc

/**
 * @author clint
 * Oct 17, 2019
 */
sealed trait AwsTool extends Tool 

object AwsTool extends ToolSyntaxCompanion[AwsTool] {
  trait HasEmptyDefaultStoresAndAnonId extends Tool {
    /** Input and output stores before any are specified using in or out */
    override def defaultStores: DefaultStores = DefaultStores.empty
    
    override val id: LId = LId.newAnonId
  }
  
  final case class UploadToS3AwsTool private 
    (src: Path, dest: URI)
    (implicit override val scriptContext: LoamScriptContext) extends AwsTool with HasEmptyDefaultStoresAndAnonId

  final case class DownloadFromS3AwsTool private 
    (src: URI, dest: Path)
    (implicit override val scriptContext: LoamScriptContext) extends AwsTool with HasEmptyDefaultStoresAndAnonId
    
  final case class PySparkAwsTool private
    (cluster: Cluster, script: URI, args: Seq[String])
    (implicit override val scriptContext: LoamScriptContext) extends AwsTool with HasEmptyDefaultStoresAndAnonId
    
  final case class RunScriptAwsTool private
    (cluster: Cluster, script: URI, args: Seq[String])
    (implicit override val scriptContext: LoamScriptContext) extends AwsTool with HasEmptyDefaultStoresAndAnonId
    
  final case class PooledAwsTool private
    (cluster: Cluster, maxClusters: Int, jobs: Seq[AwsJobDesc])
    (implicit override val scriptContext: LoamScriptContext) extends AwsTool with HasEmptyDefaultStoresAndAnonId
}
