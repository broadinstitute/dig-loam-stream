package loamstream

import java.nio.file.Path

import _root_.utils.Loggable.Level
import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal._
import loamstream.map.LToolMapper
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite}
import tools.core.{CoreConfig, CoreToolBox}
import utils.{LoamFileUtils, StringUtils, TestUtils}

import scala.io.Source

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

  val vcfFiles = Seq(StringUtils.pathTemplate(hailVcfFilePath.toString, "XXX"),
    StringUtils.pathTemplate(hailVdsFilePath.toString, "XXX"))
  val vdsFiles = Seq(hailVdsFilePath)
  val singletonFiles = Seq(hailSingletonFilePath)

  val config = CoreConfig.InteractiveFallbackConfig(vcfFiles, vdsFiles, singletonFiles)
  val genotypesId = config.genotypesId
  val vdsId = config.vdsId
  val singletonsId = config.singletonsId
  val pipeline = HailPipeline(genotypesId, vdsId, singletonsId)
  val toolbox = CoreToolBox(config) ++ MiniMockToolBox(config)
  val mappings = LToolMapper.findAllSolutions(pipeline, toolbox)
  for (mapping <- mappings)
    LToolMappingLogger.logMapping(Level.trace, mapping)
  val mappingCostEstimator = LPipelineMiniCostEstimator(pipeline.genotypesId)
  val mapping = mappingCostEstimator.pickCheapest(mappings)
  LToolMappingLogger.logMapping(Level.trace, mapping)

  val genotypesJob = toolbox.createJobs(pipeline.genotypeCallsRecipe, pipeline, mapping)
  val importVcfJob = toolbox.createJobs(pipeline.vdsRecipe, pipeline, mapping)
  val calculateSingletonsJob = toolbox.createJobs(pipeline.singletonRecipe, pipeline, mapping)

  val executable = toolbox.createExecutable(pipeline, mapping)

  test("Methods and piles are successfully mapped to tools and stores") {
    assert(mappings.size == 1)
    assert(mappings.head.stores.size == 3)
    assert(mappings.head.tools.size == 3)
  }

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
