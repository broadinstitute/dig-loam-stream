package loamstream

import java.nio.file.Path

import org.scalatest.{BeforeAndAfter, FunSuite}

import loamstream.apps.minimal.{MiniExecuter, MiniMockToolBox}
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.util.FileAsker
import loamstream.util.Loggable.Level
import loamstream.util.StringUtils
import tools.PcaWeightsReader
import tools.core.{CoreToolBox, LCoreDefaultPileIds}
import tools.core.LCoreEnv.{Keys, PathProvider, PathProviderById}
import tools.klusta.KlustaKwikKonfig
import loamstream.util.TestUtils
import loamstream.util.shot.Hit

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
final class AncestryInferenceEndToEndTest extends FunSuite with BeforeAndAfter {
  test("Going through ancestry inference pipeline without crashing.") {
    import TestData.sampleFiles

    val miniVcfFilePath = sampleFiles.miniVcfOpt.get
    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    
    val props = TestData.props
    
    val vcfFileProvider = new PathProviderById {
      override def apply(id: String): Path = FileAsker.askIfNotExist(vcfFiles.map(_ (id)))(s"VCF file '$id'")
    }
    
    val pcaWeightsFileProvider = new PathProvider {
      override def get: Path = PcaWeightsReader.weightsFilePath(props).get
    }
    
    val env = LEnv(
        Keys.vcfFilePath -> vcfFileProvider, 
        Keys.pcaWeightsFilePath -> pcaWeightsFileProvider,
        Keys.klustaKwikKonfig -> KlustaKwikKonfig.withTempWorkDir("data"),
        Keys.genotypesId -> LCoreDefaultPileIds.genotypes, 
        Keys.pcaWeightsId -> LCoreDefaultPileIds.pcaWeights)
    
    val genotypesId = env(Keys.genotypesId)
    val pcaWeightsId = env(Keys.pcaWeightsId)
    
    val pipeline = AncestryInferencePipeline(genotypesId, pcaWeightsId)
    
    val toolbox = CoreToolBox(env) ++ MiniMockToolBox(env).get

    val pcaProjectionJobsShot = toolbox.createJobs(pipeline.pcaProjectionRecipe, pipeline)
    
    //NB: Try a pattern-match to get a better error message on failures
    val Hit(Seq(pcaProjectionJob)) = pcaProjectionJobsShot.map(_.toSeq)
    
    val sampleClusteringJobsShot = toolbox.createJobs(pipeline.sampleClustering, pipeline)

    //NB: Try a pattern-match to get a better error message on failures
    val Hit(Seq(sampleClusteringJob)) = sampleClusteringJobsShot.map(_.toSeq)
    
    val executable = toolbox.createExecutable(pipeline)
    
    val readyToExecute = false
    
    if (readyToExecute) {
      val jobResults = MiniExecuter.execute(executable)
      assert(jobResults.size === 4)
    }
  }

}
