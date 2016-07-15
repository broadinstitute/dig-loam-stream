package loamstream

import loamstream.model.execute.ChunkedExecuter
import loamstream.model.jobs.{LJob, LToolBox}
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.tools.PcaWeightsReader
import loamstream.tools.core.LCoreEnv.Keys
import loamstream.tools.core.{CoreToolBox, LCoreDefaultStoreIds}
import loamstream.tools.klusta.{KlustaKwikKonfig, KlustaKwikLineCommand}
import loamstream.util.{Hit, Shot, StringUtils}
import org.scalatest.FunSuite

import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
final class AncestryInferenceEndToEndTest extends FunSuite {

  private val executer = ChunkedExecuter.default

  ignore("Running ancestry inference pipeline. (AST)") {
    val (toolbox, pipeline) = makeToolBoxAndPipeline()

    val executable = toolbox.createExecutable(pipeline.ast)

    val jobResults = executer.execute(executable)

    checkResults(jobResults)
  }

  private def checkResults(results: Map[LJob, Shot[LJob.Result]]): Unit = {
    // TODO: Should be able to calculate the number of results expected form number of jobs
    val numResultsExpected = 4 //scalastyle:ignore

    assert(results.size === numResultsExpected)

    val numSuccesses = results.count { case (_, resultShot) => isSuccessShot(resultShot) }
    
    assert(numSuccesses === 4)
  }

  private def isSuccessShot(resultShot: Shot[LJob.Result]): Boolean = resultShot match {
    case Hit(r) => r.isSuccess
    case _ => false
  }

  private def makeToolBoxAndPipeline(): (LToolBox, AncestryInferencePipeline) = {

    val miniVcfFilePath = TestData.sampleFiles.miniVcfOpt.get

    val props = TestData.props

    val pcaWeightsFile = PcaWeightsReader.weightsFilePath(props).get

    val klustaConfig = KlustaKwikKonfig.withTempWorkDir("data")

    val env = LEnv(
      Keys.genotypesId -> LCoreDefaultStoreIds.genotypes,
      Keys.pcaWeightsId -> LCoreDefaultStoreIds.pcaWeights)

    val pipeline = AncestryInferencePipeline(miniVcfFilePath, pcaWeightsFile, klustaConfig)
    val toolbox = CoreToolBox(env)

    (toolbox, pipeline)
  }

  //Returns true if the command for KlustaKwik, as specified by KlustaKwikLineCommand.name is on the path
  //and runnable; returns false otherwise.
  private def isKlustaKwikPresent: Boolean = {
    import scala.sys.process._

    val command = KlustaKwikLineCommand.name

    val processLogger = ProcessLogger(_ => (), _ => ())

    Try(command.!(processLogger)).isSuccess
  }
}
