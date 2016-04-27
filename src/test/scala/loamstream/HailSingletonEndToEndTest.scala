package loamstream

import java.nio.file.Path

import scala.io.Source

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal._
import loamstream.util.LoamFileUtils
import loamstream.util.Loggable.Level
import loamstream.util.StringUtils
import tools.core.{CoreToolBox, LCoreDefaultPileIds, LCoreEnv}
import loamstream.util.TestUtils

/**
  * Created by kyuksel on 2/29/2016.
  */
final class HailSingletonEndToEndTest extends FunSuite with BeforeAndAfter {

  import TestData.sampleFiles

  val hailVdsFilePath = sampleFiles.hailVdsOpt.get
  val hailVcfFilePath = sampleFiles.hailVcfOpt.get
  val hailSingletonFilePath = sampleFiles.singletonsOpt.get

  private def deleteQuietly(path: Path): Unit = FileUtils.deleteQuietly(path.toFile)

  // Make sure to not mistakenly use an output file from a previous run, if any
  deleteQuietly(hailVdsFilePath)
  deleteQuietly(hailSingletonFilePath)

  val vcfFiles = Seq(
                  StringUtils.pathTemplate(hailVcfFilePath.toString, "XXX"),
                  StringUtils.pathTemplate(hailVdsFilePath.toString, "XXX"))
  val vdsFiles = Seq(hailVdsFilePath)
  val singletonFiles = Seq(hailSingletonFilePath)

  val env = LCoreEnv.FileInteractiveFallback.env(vcfFiles, vdsFiles, singletonFiles) +
    (LCoreEnv.Keys.genotypesId -> LCoreDefaultPileIds.genotypes) +
    (LCoreEnv.Keys.vdsId -> LCoreDefaultPileIds.vds) +
    (LCoreEnv.Keys.singletonsId -> LCoreDefaultPileIds.singletons)
  val genotypesId = env(LCoreEnv.Keys.genotypesId)
  val vdsId = env(LCoreEnv.Keys.vdsId)
  val singletonsId = env(LCoreEnv.Keys.singletonsId)
  val pipeline = HailPipeline(genotypesId, vdsId, singletonsId)
  val toolbox = CoreToolBox(env) ++ MiniMockToolBox(env).get

  val genotypesJob = toolbox.createJobs(pipeline.genotypeCallsRecipe, pipeline)
  val importVcfJob = toolbox.createJobs(pipeline.vdsRecipe, pipeline)
  val calculateSingletonsJob = toolbox.createJobs(pipeline.singletonRecipe, pipeline)

  val executable = toolbox.createExecutable(pipeline)

  test("Jobs are successfully created") {
    assert(TestUtils.isHitOfSetOfOne(genotypesJob))
    assert(TestUtils.isHitOfSetOfOne(calculateSingletonsJob))
    assert(TestUtils.isHitOfSetOfOne(importVcfJob))
  }

  ignore("Singletons are successfully counted using Hail") {
    MiniExecuter.execute(executable)

    val source = Source.fromFile(hailSingletonFilePath.toFile)
    LoamFileUtils.enclosed(source) { bufSrc =>
      val singletonCounts = bufSrc.getLines().toList
      assert(singletonCounts.size == 101)
      assert(singletonCounts.head == "SAMPLE\tSINGLETONS")
      assert(singletonCounts.tail.head == "C1046::HG02024\t0")
      assert(singletonCounts.last == "HG00629\t0")
    }
  }

  // Make sure to not mistakenly use an output file from a previous run, if any
  deleteQuietly(hailVdsFilePath)
  deleteQuietly(hailSingletonFilePath)
}
