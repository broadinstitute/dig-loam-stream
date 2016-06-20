package loamstream.apps.minimal

import java.nio.file.Path

import loamstream.model.{AST, HasAst, LPipeline, Store, Tool}
import loamstream.tools.core.CoreTool

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
final case class MiniPipeline(vcfFile: Path, sampleIdsFile: Path) extends LPipeline with HasAst {
  val genotypeCallsTool: Tool = CoreTool.CheckPreExistingVcfFile(vcfFile)

  val sampleIdsTool: Tool = CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleIdsFile)

  override val tools: Set[Tool] = Set(genotypeCallsTool, sampleIdsTool)

  override def stores: Set[Store] = tools.flatMap(_.outputs.values)

  override lazy val ast: AST = {
    import Tool.ParamNames.{input, output}

    val genotypeCallsNode = AST(genotypeCallsTool)

    AST(sampleIdsTool).get(input).from(genotypeCallsNode(output))
  }
}
