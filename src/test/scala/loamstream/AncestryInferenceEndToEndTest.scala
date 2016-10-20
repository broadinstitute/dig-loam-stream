package loamstream

import scala.sys.process.stringToProcess
import scala.util.Try

import org.scalatest.FunSuite

import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LToolBox
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.tools.PcaWeightsReader
import loamstream.tools.core.CoreToolBox
import loamstream.tools.klusta.KlustaKwikKonfig
import loamstream.tools.klusta.KlustaKwikLineCommand
import loamstream.util.Hit
import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
final class AncestryInferenceEndToEndTest extends FunSuite {

  private val executer = RxExecuter.default

  ignore("Running ancestry inference pipeline. (AST)") {
    val (toolbox, pipeline) = makeToolBoxAndPipeline()

    val executable = toolbox.createExecutable(pipeline.ast)

    val jobResults = executer.execute(executable)

    checkResults(jobResults)
  }

  private def checkResults(results: Map[LJob, JobState]): Unit = {
    // TODO: Should be able to calculate the number of results expected form number of jobs
    val numResultsExpected = 4 //scalastyle:ignore

    assert(results.size === numResultsExpected)

    val numSuccesses = results.values.count(_.isSuccess)
    
    assert(numSuccesses === 4)
  }

  private def isSuccessShot(resultShot: Shot[JobState]): Boolean = resultShot match {
    case Hit(r) => r.isSuccess
    case _ => false
  }

  private def makeToolBoxAndPipeline(): (LToolBox, AncestryInferencePipeline) = {

    val miniVcfFilePath = TestData.sampleFiles.miniVcfOpt.get

    val props = TestData.props

    val pcaWeightsFile = PcaWeightsReader.weightsFilePath(props).get

    val klustaConfig = KlustaKwikKonfig.withTempWorkDir("data")

    val pipeline = AncestryInferencePipeline(miniVcfFilePath, pcaWeightsFile, klustaConfig)
    val toolbox = CoreToolBox

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
