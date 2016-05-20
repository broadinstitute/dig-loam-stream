package loamstream.apps.minimal

import loamstream.model.LPipeline
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.tools.core.CoreTool
import loamstream.model.AST
import loamstream.model.ToolSpec

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
case class MiniPipeline(genotypesId: String) extends LPipeline {
  val genotypeCallsTool: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val sampleIdsTool: Tool = CoreTool.extractSampleIdsFromVcfFile

  override val tools: Set[Tool] = Set(genotypeCallsTool, sampleIdsTool)
  
  override def stores: Set[Store] = tools.flatMap(_.outputs.values)
  
  lazy val ast: AST = {
    import ToolSpec.ParamNames.{ input, output }
    
    val genotypeCallsNode = AST(genotypeCallsTool)
    
    AST(sampleIdsTool).get(input).from(genotypeCallsNode(output))
  }
}
