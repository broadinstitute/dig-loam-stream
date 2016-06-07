package loamstream.pipelines.qc.ancestry

import loamstream.Sigs

import loamstream.model.LPipeline
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.values.LType._
import loamstream.tools.core.CoreStore
import loamstream.tools.core.CoreTool
import loamstream.model.AST
import loamstream.model.ToolSpec
import loamstream.tools.klusta.KlustaKwikKonfig
import java.nio.file.Path
import loamstream.model.HasAst


/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
final case class AncestryInferencePipeline(
    vcfFile: Path, 
    pcaWeightsFile: Path, 
    klustaConfig: KlustaKwikKonfig) extends LPipeline with HasAst {

  val genotypesTool: Tool = CoreTool.CheckPreExistingVcfFile(vcfFile)
  
  val pcaWeightsTool: Tool = CoreTool.CheckPreExistingPcaWeightsFile(pcaWeightsFile)
  
  val pcaProjectionTool: Tool = CoreTool.ProjectPca(vcfFile, pcaWeightsFile, klustaConfig)
  
  val sampleClusteringTool: Tool = CoreTool.ClusteringSamplesByFeatures(klustaConfig)
  
  override def stores: Set[Store] = tools.flatMap(_.outputs.values)
  
  override val tools: Set[Tool] = Set(genotypesTool, pcaWeightsTool, pcaProjectionTool, sampleClusteringTool)
  
  override lazy val ast: AST = {
    
    val pcaWeightsNode = AST(pcaWeightsTool)
    val genotypesNode = AST(genotypesTool)
    
    import ToolSpec.ParamNames.{ input, output }

    //TODO: sort out names
    val vcfFileInput = CoreStore.vcfFile.id
    val pcaWeightsFileInput = CoreStore.pcaWeightsFile.id
    
    val pcaProjectionNode = {
      val toolNode = AST(pcaProjectionTool)
      
      val withVcfInput = toolNode.connect(vcfFileInput).to(genotypesNode(output))
      
      withVcfInput.connect(pcaWeightsFileInput).to(pcaWeightsNode(output)) 
    }
    
    AST(sampleClusteringTool).connect(input).to(pcaProjectionNode(output))
  }
}
