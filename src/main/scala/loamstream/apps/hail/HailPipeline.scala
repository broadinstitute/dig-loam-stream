package loamstream.apps.hail

import loamstream.model.AST
import loamstream.model.HasAst
import loamstream.model.LPipeline
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.tools.core.CoreTool
import loamstream.model.ToolSpec
import java.nio.file.Path

/**
  * Created on: 3/10/2016
  *
  * @author Kaan Yuksel
  */
final case class HailPipeline(vcfFile: Path, vdsDir: Path, singletonsFile: Path) extends LPipeline with HasAst {
  val genotypeCallsTool: Tool = CoreTool.CheckPreExistingVcfFile(vcfFile)
  
  val vdsTool: Tool = CoreTool.ConvertVcfToVds(vcfFile, vdsDir)
  
  val singletonTool: Tool = CoreTool.CalculateSingletons(vdsDir, singletonsFile)

  override def stores: Set[Store] = tools.flatMap(_.outputs.values)
  
  override val tools: Set[Tool] = Set(genotypeCallsTool, vdsTool, singletonTool)
  
  override lazy val ast: AST = {
    import ToolSpec.ParamNames.{ input, output }
    
    val genotypeCallsNode = AST(genotypeCallsTool)
    
    val vdsTree = AST(vdsTool).get(input).from(genotypeCallsNode(output))
    
    AST(singletonTool).get(input).from(vdsTree(output))
  }
}
