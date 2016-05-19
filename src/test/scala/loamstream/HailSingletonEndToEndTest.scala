package loamstream

import java.nio.file.Path

import scala.io.Source

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal._
import loamstream.util.Hit
import loamstream.util.LoamFileUtils
import loamstream.util.StringUtils
import tools.core.{CoreToolBox, LCoreDefaultStoreIds, LCoreEnv}

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
    (LCoreEnv.Keys.genotypesId -> LCoreDefaultStoreIds.genotypes) +
    (LCoreEnv.Keys.vdsId -> LCoreDefaultStoreIds.vds) +
    (LCoreEnv.Keys.singletonsId -> LCoreDefaultStoreIds.singletons)
  val genotypesId = env(LCoreEnv.Keys.genotypesId)
  val vdsId = env(LCoreEnv.Keys.vdsId)
  val singletonsId = env(LCoreEnv.Keys.singletonsId)
  val pipeline = HailPipeline(genotypesId, vdsId, singletonsId)
  val toolbox = CoreToolBox(env)

  val genotypesJobsShot = toolbox.createJobs(pipeline.genotypeCallsTool, pipeline)
  val importVcfJobsShot = toolbox.createJobs(pipeline.vdsTool, pipeline)
  val calculateSingletonsJobsShot = toolbox.createJobs(pipeline.singletonTool, pipeline)

  val executable = toolbox.createExecutable(pipeline)

  test("Jobs are successfully created") {
    //NB: Try to unpack the shots to get better messages on failures
    val Hit(Seq(genotypesJob)) = genotypesJobsShot.map(_.toSeq)
    val Hit(Seq(importVcfJob)) = importVcfJobsShot.map(_.toSeq)
    val Hit(Seq(calculateSingletonsJob)) = calculateSingletonsJobsShot.map(_.toSeq)
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
