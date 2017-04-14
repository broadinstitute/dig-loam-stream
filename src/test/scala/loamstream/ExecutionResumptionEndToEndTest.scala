package loamstream

import java.nio.file.{Path, Paths}

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.{LoamCompiler, LoamEngine}
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.execute.{DbBackedJobFilter, Executable, ExecuterHelpers, MockChunkRunner, RxExecuter}
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.Output.PathOutput
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.{Execution, LJob, Output, OutputRecord}
import loamstream.util.code.SourceUtils
import loamstream.util.{Loggable, Sequence}
import org.scalatest.{FunSuite, Matchers}
import loamstream.model.execute.AsyncLocalChunkRunner

import scala.concurrent.ExecutionContext
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.JobStatus.Skipped

/**
  * @author kaan
  * Sep 27, 2016
  */
final class ExecutionResumptionEndToEndTest extends FunSuite with ProvidesSlickLoamDao with Loggable {

  // scalastyle:off no.whitespace.before.left.bracket

  test("Jobs are skipped if their outputs were already produced by a previous run") {
    createTablesAndThen {
      import TestHelpers._

      val fileIn = Paths.get("src", "test", "resources", "a.txt")

      val workDir = makeWorkDir()

      val fileOut1 = workDir.resolve("fileOut1.txt")
      val fileOut2 = workDir.resolve("fileOut2.txt")

      /* Loam for the first run that mimics an incomplete pipeline run:
          val fileIn = store[TXT].at("src/test/resources/a.txt").asInput
          val fileOut1 = store[TXT].at("$workDir/fileOut1.txt")
          cmd"cp $$fileIn $$fileOut1
       */
      val firstScript = {
        s"""val fileIn = store[TXT].at(${SourceUtils.toStringLiteral(fileIn)}).asInput
            val fileOut1 = store[TXT].at(${SourceUtils.toStringLiteral(fileOut1)})
            cmd"cp $$fileIn $$fileOut1""""
      }

      val (executable, executions) = compileAndRun(firstScript)

      //Jobs and results come back as an unordered map, so we need to find the jobs we're looking for. 
      val firstJob = jobThatWritesTo(executable)(fileOut1).get
      val firstExecution = executions(firstJob)
      val firstResultOpt = firstExecution.result

      assert(firstResultOpt.nonEmpty)

      val firstResult = firstResultOpt.get.asInstanceOf[CommandResult]
      
      assert(firstResult.exitCode === 0)

      val firstResourcesOpt = firstExecution.resources

      assert(firstResourcesOpt.nonEmpty)

      val firstResources = firstResourcesOpt.get

      //Ignore run-dependent start and end times
      assert(firstResources.isInstanceOf[LocalResources] === true)

      assert(executions.size === 1)

      val output1 = OutputRecord(PathOutput(fileOut1))
      val output2 = OutputRecord(PathOutput(fileOut2))

      val executionFromOutput1 = dao.findExecution(output1).get
      
      val output1Result = executionFromOutput1.result.get.asInstanceOf[CommandResult]
      
      assert(output1Result.exitCode === 0)
      assert(executionFromOutput1.resources.get === firstResources)
      assert(executionFromOutput1.outputs === Set(output1))

      assert(dao.findExecution(output2) === None)

      /* Loam for the second run that mimics a run launched subsequently to an incomplete first run:
          val fileIn = store[TXT].at("src/test/resources/a.txt").asInput
          val fileOut1 = store[TXT].at("$workDir/fileOut1.txt")
          val fileOut2 = store[TXT].at("$workDir/fileOut2.txt")
          cmd"cp $$fileIn $$fileOut1"
          cmd"cp $$fileOut1 $$fileOut2
       */
      val secondScript =
      s"""val fileIn = store[TXT].at(${SourceUtils.toStringLiteral(fileIn)}).asInput
            val fileOut1 = store[TXT].at(${SourceUtils.toStringLiteral(fileOut1)})
            val fileOut2 = store[TXT].at(${SourceUtils.toStringLiteral(fileOut2)})
            cmd"cp $$fileIn $$fileOut1"
            cmd"cp $$fileOut1 $$fileOut2""""

      //Run the script and validate the results
      def run(expectedStatuses: Seq[Execution]): Unit = {
        val (executable, executions) = compileAndRun(secondScript)

        val updatedOutput1 = OutputRecord(PathOutput(fileOut1))
        val updatedOutput2 = OutputRecord(PathOutput(fileOut2))

        //Jobs and results come back as an unordered map, so we need to find the jobs we're looking for. 
        val firstJob = jobThatWritesTo(executable)(fileOut1).get
        val secondJob = jobThatWritesTo(executable)(fileOut2).get

        val firstExecution = executions(firstJob)
        val secondExecution = executions(secondJob)
        
        def compareResultsAndStatuses(actual: Execution, expected: Execution): Unit = {
          assert(actual.status === expected.status)

          assert(actual.result === expected.result)
        }
        
        compareResultsAndStatuses(firstExecution, expectedStatuses(0))
        compareResultsAndStatuses(secondExecution, expectedStatuses(1))
        
        assert(executions.size === 2)

        //If the jobs were run, we should have written an Execution for the job.
        //If the job was skipped, we should have left the one from the previous successful run alone.

        compareResultsAndStatuses(
            dao.findExecution(updatedOutput1).get,
            executionFromResult(CommandResult(0)))
        
        assert(dao.findExecution(updatedOutput1).get.outputs === Set(updatedOutput1))

        compareResultsAndStatuses(
            dao.findExecution(updatedOutput2).get,
          executionFromResult(CommandResult(0)))
        
        assert(dao.findExecution(updatedOutput2).get.outputs === Set(updatedOutput2))
      }

      //Run the second script a few times.  The first time, we expect the first job to be skipped, and the second one
      //to be run.  We expect both jobs to be skipped in all subsequent runs.
      run(Seq(executionFrom(Skipped), executionFromResult(CommandResult(0))))
      
      run(Seq(Skipped, Skipped).map(executionFrom(_)))
      run(Seq(Skipped, Skipped).map(executionFrom(_)))
      run(Seq(Skipped, Skipped).map(executionFrom(_)))
    }
  }

  test("Single failed job's exit status is recorded properly") {
    createTablesAndThen {
      val fileIn = Paths.get("src", "test", "resources", "a.txt")

      val workDir = makeWorkDir()

      val fileOut1 = workDir.resolve("fileOut1.txt")

      val bogusCommandName = "asdfasdf"

      /* Loam for a single invocation of a bogus command:
          val fileIn = store[TXT].at("src/test/resources/a.txt").asInput
          val fileOut1 = store[TXT].at("$workDir/fileOut1.txt")
          cmd"asdfasdf $$fileIn $$fileOut1"
       */
      val script = {
        s"""val fileIn = store[TXT].at(${SourceUtils.toStringLiteral(fileIn)}).asInput
            val fileOut1 = store[TXT].at(${SourceUtils.toStringLiteral(fileOut1)})
            cmd"$bogusCommandName $$fileIn $$fileOut1""""
      }

      //Run the script and validate the results
      def run(): Unit = {
        val (executable, executions) = compileAndRun(script)

        val allJobs = allJobsFrom(executable)

        val allCommandLines = allJobs.collect { case clj: CommandLineJob => clj.commandLineString }

        assert(allCommandLines.map(_.take(bogusCommandName.length)) === Seq(bogusCommandName))

        val onlyExecution = executions.values.head

        val exitCode = 127
        val onlyResultOpt = onlyExecution.result

        assert(onlyResultOpt.nonEmpty)

        val onlyResult = onlyResultOpt.get

        {
          import Matchers._

          executions should have size 1
          onlyExecution.resources.get.isInstanceOf[LocalResources] shouldBe true
          onlyResult.asInstanceOf[CommandResult].exitCode shouldEqual exitCode
          onlyResult.isFailure shouldBe true
        }

        val output1 = OutputRecord(fileOut1)
        val recordOpt = dao.findOutputRecord(output1)

        assert(recordOpt === Some(output1))
        assert(recordOpt.get.hash.isEmpty)

        assert(dao.findExecution(output1).get.result.get.isFailure)
        assert(dao.findExecution(output1).get.outputs === Set(output1))
      }

      //Run the script a few times; we expect that the executer will try to run the bogus job every time, 
      //since it never succeeds.
      run()
      run()
      run()
    }
  }

  test("Exit status of failed jobs are recorded properly") {
    createTablesAndThen {
      val fileIn = Paths.get("src", "test", "resources", "a.txt")

      val workDir = makeWorkDir()

      val fileOut1 = workDir.resolve("fileOut1.txt")
      val fileOut2 = workDir.resolve("fileOut2.txt")

      val bogusCommandName = "asdfasdf"

      /* Loam for a run with two bogus jobs:
          val fileIn = store[TXT].at("src/test/resources/a.txt").asInput
          val fileOut1 = store[TXT].at("$workDir/fileOut1.txt")
          val fileOut2 = store[TXT].at("$workDir/fileOut2.txt")
          cmd"asdfasdf $$fileIn $$fileOut1"
          cmd"asdfasdf $$fileOut1 $$fileOut2"
       */
      val script =
        s"""val fileIn = store[TXT].at(${SourceUtils.toStringLiteral(fileIn)}).asInput
              val fileOut1 = store[TXT].at(${SourceUtils.toStringLiteral(fileOut1)})
              val fileOut2 = store[TXT].at(${SourceUtils.toStringLiteral(fileOut2)})
              cmd"$bogusCommandName $$fileIn $$fileOut1"
              cmd"$bogusCommandName $$fileOut1 $$fileOut2""""

      //Run the script and validate the results
      def run(): Unit = {
        val (executable, executions) = compileAndRun(script)

        val firstJob = jobThatWritesTo(executable)(fileOut1).get
        val secondJob = jobThatWritesTo(executable)(fileOut2).get

        {
          import Matchers._

          val exitCode = 127
          executions(firstJob).result.get.asInstanceOf[CommandResult].exitCode shouldEqual exitCode
          executions(firstJob).resources.get.isInstanceOf[LocalResources] shouldBe true
          executions.contains(secondJob) shouldBe false

          executions(firstJob).result.get.isFailure shouldBe true

          executions should have size 1
        }

        val output1 = OutputRecord(fileOut1)
        val output2 = OutputRecord(fileOut2)

        assert(dao.findExecution(output1).get.result.get.isFailure)
        assert(dao.findExecution(output1).get.outputs === Set(output1))

        //NB: The job that referenced output2 didn't get run, so its execution should not have been recorded 
        assert(dao.findExecution(output2) === None)
      }

      //Run the script a few times; we expect that the executer will try to run both (bogus) jobs every time, 
      //since they never succeed.
      run()
      run()
      run()
    }
  }

  private val sequence: Sequence[Int] = Sequence()

  private def makeWorkDir(): Path = {
    def exists(path: Path): Boolean = path.toFile.exists

    val suffixes = sequence.iterator

    val candidates = suffixes.map(i => Paths.get("target", s"resumptive-executer-test$i"))

    val result = candidates.dropWhile(exists).next()

    val asFile = result.toFile

    asFile.mkdir()

    assert(asFile.exists)

    result
  }

  private val resumptiveExecuter = {
    val dbBackedJobFilter = new DbBackedJobFilter(dao)

    RxExecuter.defaultWith(dbBackedJobFilter)
  }

  private def makeLoggingExecuter: (RxExecuter, MockChunkRunner) = {
    val asyncChunkRunner = AsyncLocalChunkRunner()(ExecutionContext.global)

    val mockRunner = MockChunkRunner(asyncChunkRunner)

    val resultExecuter = {
      resumptiveExecuter.copy(runner = mockRunner, maxRunsPerJob = 0)(resumptiveExecuter.executionContext)
    }
    
    (resultExecuter, mockRunner)
  }

  private def loamEngine = {
    val outMessageSink = LoggableOutMessageSink(this)

    val (executer, _) = makeLoggingExecuter

    LoamEngine(TestHelpers.config, LoamCompiler(outMessageSink), executer, outMessageSink)
  }

  private def compileAndRun(script: String): (Executable, Map[LJob, Execution]) = {
    val engine = loamEngine

    val executable = engine.compileToExecutable(script).get

    val executions = engine.executer.execute(executable)

    (executable, executions)
  }

  private def allJobsFrom(executable: Executable): Seq[LJob] = ExecuterHelpers.flattenTree(executable.jobs).toSeq

  private def jobThatWritesTo(executable: Executable)(fileNameSuffix: Path): Option[LJob] = {
    val allJobs = allJobsFrom(executable)

    def outputMatches(o: Output): Boolean =
      o.asInstanceOf[Output.PathOutput].path.toString.endsWith(fileNameSuffix.toString)

    def jobMatches(j: LJob): Boolean = j.outputs.exists(outputMatches)

    allJobs.find(jobMatches)
  }

  // scalastyle:on no.whitespace.before.left.bracket
}
