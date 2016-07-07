package loamstream

import java.nio.file.Path

import scala.io.Source

import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal._
import loamstream.model.execute.LExecutable
import loamstream.model.execute.ChunkedExecuter
import loamstream.util.Hit
import loamstream.util.LoamFileUtils
import loamstream.util.StringUtils
import tools.core.{CoreToolBox, LCoreDefaultStoreIds, LCoreEnv}

/**
  * Created by kyuksel on 2/29/2016.
  */
final class HailSingletonEndToEndTest extends FunSuite {

  ignore("Singletons are successfully counted using Hail (AST)") {
    doTestExecutable((toolbox, pipeline) => toolbox.createExecutable(pipeline.ast))
  }
  
  private def doTestExecutable(makeExecutable: (CoreToolBox, HailPipeline) => LExecutable): Unit = {
    val (toolbox, pipeline, Paths(hailVdsFilePath, hailSingletonFilePath)) = makeToolboxAndPipeline()
    
    deleteSampleFilesBeforeAndAfter(hailVdsFilePath, hailSingletonFilePath) {
      val executable = makeExecutable(toolbox, pipeline)
    
      val executer = ChunkedExecuter.default
      
      val results = executer.execute(executable)
      
      assert(results.size == 3)
      
      //TODO: More-explicit test for better message on failures.
      results.values.forall {
        case Hit(r) => r.isSuccess
        case _ => false
      }
      
      val source = Source.fromFile(hailSingletonFilePath.toFile)
    
      val singletonCounts = LoamFileUtils.enclosed(source)(_.getLines.toList)
      
      assert(singletonCounts.size == 101)
      assert(singletonCounts.head == "SAMPLE\tSINGLETONS")
      assert(singletonCounts.tail.head == "C1046::HG02024\t0")
      assert(singletonCounts.last == "HG00629\t0")
    }
  }
  
  private final case class Paths(hailVdsFilePath: Path, hailSingletonFilePath: Path)
  
  private def makeToolboxAndPipeline(): (CoreToolBox, HailPipeline, Paths) = {
    
    import TestData.sampleFiles
    
    val hailVdsFilePath = sampleFiles.hailVdsOpt.get
    val hailVcfFilePath = sampleFiles.hailVcfOpt.get
    val hailSingletonFilePath = sampleFiles.singletonsOpt.get

    val vcfFiles = Seq(
      StringUtils.pathTemplate(hailVcfFilePath.toString, "XXX"),
      StringUtils.pathTemplate(hailVdsFilePath.toString, "XXX"))
  
    val vdsFiles = Seq(hailVdsFilePath)
    val singletonFiles = Seq(hailSingletonFilePath)

    val env = LCoreEnv.FileInteractiveFallback.env(vcfFiles, vdsFiles, singletonFiles) +
      (LCoreEnv.Keys.genotypesId -> LCoreDefaultStoreIds.genotypes) +
      (LCoreEnv.Keys.vdsId -> LCoreDefaultStoreIds.vds) +
      (LCoreEnv.Keys.singletonsId -> LCoreDefaultStoreIds.singletons)
    
    val pipeline = HailPipeline(hailVcfFilePath, hailVdsFilePath, hailSingletonFilePath)
    
    val toolbox = CoreToolBox(env)

    (toolbox, pipeline, Paths(hailVdsFilePath, hailSingletonFilePath))
  }
  
  private def deleteSampleFilesBeforeAndAfter[A](files: Path*)(f: => A): A = {
    def deleteQuietly(path: Path): Unit = FileUtils.deleteQuietly(path.toFile)
    
    try { 
      // Make sure to not mistakenly use an output file from a previous run, if any
      files.foreach(deleteQuietly)
      
      f
    } finally {
      // Make sure to not mistakenly use an output file from a previous run, if any
      files.foreach(deleteQuietly)
    }
  }
}
