package loamstream.apps.hail

import java.nio.file.Path

import loamstream.model.{AST, HasAst, LPipeline, Store, Tool}
import loamstream.tools.core.CoreTool

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
    import Tool.ParamNames.{input, output}

    val genotypeCallsNode = AST(genotypeCallsTool)

    val vdsTree = AST(vdsTool).connect(input).to(genotypeCallsNode(output))

    AST(singletonTool).connect(input).to(vdsTree(output))
  }
}
