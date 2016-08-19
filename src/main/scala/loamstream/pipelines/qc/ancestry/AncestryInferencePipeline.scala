package loamstream.pipelines.qc.ancestry

import java.nio.file.Path

import loamstream.model.{ AST, HasAst, Tool }
import loamstream.tools.core.{CoreStore, CoreTool}
import loamstream.tools.klusta.KlustaKwikKonfig


/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
final case class AncestryInferencePipeline(
                                            vcfFile: Path,
                                            pcaWeightsFile: Path,
                                            klustaConfig: KlustaKwikKonfig) extends HasAst {

  val genotypesTool: Tool = CoreTool.CheckPreExistingVcfFile(vcfFile)

  val pcaWeightsTool: Tool = CoreTool.CheckPreExistingPcaWeightsFile(pcaWeightsFile)

  val pcaProjectionTool: Tool = CoreTool.ProjectPca(vcfFile, pcaWeightsFile, klustaConfig)

  val sampleClusteringTool: Tool = CoreTool.ClusteringSamplesByFeatures(klustaConfig)

  override lazy val ast: AST = {

    val pcaWeightsNode = AST(pcaWeightsTool)
    val genotypesNode = AST(genotypesTool)

    import Tool.ParamNames.{input, output}

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
