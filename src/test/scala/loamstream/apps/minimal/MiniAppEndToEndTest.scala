package loamstream.apps.minimal

import java.nio.file.{Files, Path}

import scala.io.Source

import org.scalatest.FunSuite

import loamstream.TestData
import loamstream.tools.core.{CoreToolBox, LCoreEnv}
import loamstream.tools.core.LCoreDefaultStoreIds
import loamstream.util.LoamFileUtils
import loamstream.util.Loggable.Level
import loamstream.util.StringUtils
import loamstream.util.TestUtils
import loamstream.util.Hit
import loamstream.util.Shot
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LToolBox
import loamstream.model.execute.LExecutable

/**
  * Created by kyuksel on 2/29/2016.
  */
final class MiniAppEndToEndTest extends FunSuite {
  test("Pipeline successfully extracts sample IDs from VCF") {
    val (toolbox, pipeline, extractedSamplesFilePath) = makePipelineAndToolbox()

    val genotypesJob = toolbox.createJobs(pipeline.genotypeCallsTool, pipeline)
    
    assert(TestUtils.isHitOfSetOfOne(genotypesJob))
    
    val extractSamplesJobShot = toolbox.createJobs(pipeline.sampleIdsTool, pipeline)
    
    assert(TestUtils.isHitOfSetOfOne(extractSamplesJobShot))
    
    val executable = toolbox.createExecutable(pipeline)
    
    val results = MiniExecuter.execute(executable)

    doTestExecutable(executable, extractedSamplesFilePath)
  }
  
  test("Pipeline successfully extracts sample IDs from VCF (via AST)") {
    val (toolbox, pipeline, extractedSamplesFilePath) = makePipelineAndToolbox()

    val executable = toolbox.createExecutable(pipeline.ast)
    
    doTestExecutable(executable, extractedSamplesFilePath)
  }
  
  private def doTestExecutable(executable: LExecutable, extractedSamplesFilePath: Path): Unit = {
    val results = MiniExecuter.execute(executable)
    
    assert(results.size == 2)
    
    assert(results.values.forall {
      case Hit(r) => r.isSuccess
      case _ => false
    })

    val source = Source.fromFile(extractedSamplesFilePath.toFile)
    
    val samples = LoamFileUtils.enclosed(source)(_.getLines.toList)
    
    val expectedSamplesList = List("Sample1", "Sample2", "Sample3")
    
    assert(samples == expectedSamplesList)
  }
  
  private def makePipelineAndToolbox(): (LToolBox, MiniPipeline, Path) = {
    import TestData.sampleFiles

    val miniVcfFilePath = sampleFiles.miniVcfOpt.get
    val extractedSamplesFilePath = Files.createTempFile("samples", "txt")

    val vcfFiles = Seq(StringUtils.pathTemplate(miniVcfFilePath.toString, "XXX"))
    val sampleFilePaths = Seq(extractedSamplesFilePath)

    val env = {
      LCoreEnv.FileInteractiveFallback.env(vcfFiles, sampleFilePaths, Seq.empty[Path]) +
      (LCoreEnv.Keys.genotypesId -> LCoreDefaultStoreIds.genotypes)
    }

    val genotypesId = env(LCoreEnv.Keys.genotypesId)
    val pipeline = MiniPipeline(miniVcfFilePath, extractedSamplesFilePath)
    val toolbox = CoreToolBox(env)
    
    (toolbox, pipeline, extractedSamplesFilePath)
  }
}
