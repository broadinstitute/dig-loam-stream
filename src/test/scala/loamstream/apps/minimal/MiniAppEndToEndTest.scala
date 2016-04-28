package loamstream.apps.minimal

import java.nio.file.{Files, Path}

import scala.io.Source

import org.scalatest.FunSuite

import loamstream.TestData
import loamstream.tools.core.{CoreToolBox, LCoreEnv}
import loamstream.tools.core.LCoreDefaultPileIds
import loamstream.util.LoamFileUtils
import loamstream.util.Loggable.Level
import loamstream.util.StringUtils
import loamstream.util.TestUtils
import loamstream.util.Hit
import loamstream.util.Shot
import loamstream.model.jobs.LJob

/**
  * Created by kyuksel on 2/29/2016.
  */
final class MiniAppEndToEndTest extends FunSuite {
  test("Pipeline successfully extracts sample IDs from VCF") {
    import TestData.sampleFiles

    val miniVcfFilePath = sampleFiles.miniVcfOpt.get
    val extractedSamplesFilePath = Files.createTempFile("samples", "txt")

    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    val sampleFilePaths = Seq(extractedSamplesFilePath)

    val env = {
      LCoreEnv.FileInteractiveFallback.env(vcfFiles, sampleFilePaths, Seq.empty[Path]) +
      (LCoreEnv.Keys.genotypesId -> LCoreDefaultPileIds.genotypes)
    }

    val genotypesId = env(LCoreEnv.Keys.genotypesId)
    val pipeline = MiniPipeline(genotypesId)
    val toolbox = CoreToolBox(env) ++ MiniMockToolBox(env).get

    val genotypesJob = toolbox.createJobs(pipeline.genotypeCallsTool, pipeline)
    
    assert(TestUtils.isHitOfSetOfOne(genotypesJob))
    
    val extractSamplesJobShot = toolbox.createJobs(pipeline.sampleIdsTool, pipeline)
    
    assert(TestUtils.isHitOfSetOfOne(extractSamplesJobShot))
    
    val executable = toolbox.createExecutable(pipeline)
    
    val results = MiniExecuter.execute(executable)

    val source = Source.fromFile(extractedSamplesFilePath.toFile)
    
    LoamFileUtils.enclosed(source) { bufSrc =>
      val extractedSamplesList = bufSrc.getLines.toList
      val expectedSamplesList = List("Sample1", "Sample2", "Sample3")
      assert(extractedSamplesList == expectedSamplesList)
    }
  }
}
