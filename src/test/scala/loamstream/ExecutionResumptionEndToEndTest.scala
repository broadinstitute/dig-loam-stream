package loamstream

import java.nio.file.{Path, Paths}

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.{LoamCompiler, LoamEngine}
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.execute.{DbBackedJobFilter, Executable, ExecuterHelpers, MockChunkRunner, RxExecuter}
import loamstream.model.jobs.JobState.{CommandResult, Skipped}
import loamstream.model.jobs.Output.PathOutput
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.{JobState, LJob, Output}
import loamstream.util.code.SourceUtils
import loamstream.util.{Loggable, Sequence}
import org.scalatest.{FunSuite, Matchers}
import loamstream.model.execute.AsyncLocalChunkRunner

/**
  * @author kaan
  *         date: Sep 27, 2016
  */
final class ExecutionResumptionEndToEndTest extends FunSuite with ProvidesSlickLoamDao with Loggable {

  // scalastyle:off no.whitespace.before.left.bracket

  test("Jobs are skipped if their outputs were already produced by a previous run") {
    createTablesAndThen {
      val fileIn = Paths.get("src", "test", "resources", "a.txt")

      val workDir = makeWorkDir()

      val fileOut1 = workDir.resolve("fileOut1.txt")
      val fileOut2 = workDir.resolve("fileOut2.txt")

      val output1 = PathOutput(fileOut1)
      val output2 = PathOutput(fileOut2)

      /* Loam for the first run that mimics an incomplete pipeline run:
          val fileIn = store[TXT].from("src/test/resources/a.txt")
          val fileOut1 = store[TXT].to("$workDir/fileOut1.txt")
          cmd"cp $$fileIn $$fileOut1
       */
      val firstScript =
      s"""val fileIn = store[TXT].from(${SourceUtils.toStringLiteral(fileIn)})
            val fileOut1 = store[TXT].to(${SourceUtils.toStringLiteral(fileOut1)})
            cmd"cp $$fileIn $$fileOut1""""

      val (executable, results) = compileAndRun(firstScript)

      //Jobs and results come back as an unordered map, so we need to find the jobs we're looking for. 
      val firstJob = jobThatWritesTo(executable)(fileOut1).get

      assert(results(firstJob) === CommandResult(0))

      assert(results.size === 1)

      assert(dao.findExecution(output1).get.exitState === CommandResult(0))
      assert(dao.findExecution(output1).get.outputs === Set(output1.normalized.toCachedOutput))

      assert(dao.findExecution(output2) === None)

      /* Loam for the second run that mimics a run launched subsequently to an incomplete first run:
          val fileIn = store[TXT].from("src/test/resources/a.txt")
          val fileOut1 = store[TXT].to("$workDir/fileOut1.txt")
          val fileOut2 = store[TXT].to("$workDir/fileOut2.txt")
          cmd"cp $$fileIn $$fileOut1"
          cmd"cp $$fileOut1 $$fileOut2
       */
      val secondScript =
      s"""val fileIn = store[TXT].from(${SourceUtils.toStringLiteral(fileIn)})
            val fileOut1 = store[TXT].to(${SourceUtils.toStringLiteral(fileOut1)})
            val fileOut2 = store[TXT].to(${SourceUtils.toStringLiteral(fileOut2)})
            cmd"cp $$fileIn $$fileOut1"
            cmd"cp $$fileOut1 $$fileOut2""""

      //Run the script and validate the results
      def run(expectedStates: Seq[JobState]): Unit = {
        val (executable, results) = compileAndRun(secondScript)

        //Jobs and results come back as an unordered map, so we need to find the jobs we're looking for. 
        val firstJob = jobThatWritesTo(executable)(fileOut1).get
        val secondJob = jobThatWritesTo(executable)(fileOut2).get

        assert(results(firstJob) === expectedStates(0))
        assert(results(secondJob) === expectedStates(1))

        assert(results.size === 2)

        //If the jobs were run or skipped, we should have written an Execution for the job. If the job was skipped,
        //left the one from the previous successful run alone. 
        assert(dao.findExecution(output1).get.exitState === CommandResult(0))
        assert(dao.findExecution(output1).get.outputs === Set(output1.normalized.toCachedOutput))

        assert(dao.findExecution(output2).get.exitState === CommandResult(0))
        assert(dao.findExecution(output2).get.outputs === Set(output2.normalized.toCachedOutput))
      }

      //Run the second script a few times.  The first time, we expect the first job to be skipped, and the second one
      //to be run.  We expect both jobs to be skipped in all subsequent runs.
      run(Seq(Skipped, CommandResult(0)))
      run(Seq(Skipped, Skipped))
      run(Seq(Skipped, Skipped))
      run(Seq(Skipped, Skipped))
    }
  }

  test("Single failed job's exit status is recorded properly") {
    createTablesAndThen {
      val fileIn = Paths.get("src", "test", "resources", "a.txt")

      val workDir = makeWorkDir()

      val fileOut1 = workDir.resolve("fileOut1.txt")

      val bogusCommandName = "asdfasdf"

      /* Loam for a single invocation of a bogus command:
          val fileIn = store[TXT].from("src/test/resources/a.txt")
          val fileOut1 = store[TXT].to("$workDir/fileOut1.txt")
          cmd"asdfasdf $$fileIn $$fileOut1"
       */
      val script = {
        s"""val fileIn = store[TXT].from(${SourceUtils.toStringLiteral(fileIn)})
            val fileOut1 = store[TXT].to(${SourceUtils.toStringLiteral(fileOut1)})
            cmd"$bogusCommandName $$fileIn $$fileOut1""""
      }

      //Run the script and validate the results
      def run(): Unit = {
        val (executable, jobStates) = compileAndRun(script)

        val allJobs = allJobsFrom(executable)

        val allCommandLines = allJobs.collect { case clj: CommandLineJob => clj.commandLineString }

        assert(allCommandLines.map(_.take(bogusCommandName.length)) === Seq(bogusCommandName))

        {
          import Matchers._

          val onlyResult = jobStates.values.head

          val exitCode = 127
          onlyResult shouldEqual CommandResult(exitCode)
          onlyResult.isFailure shouldBe true

          jobStates should have size 1
        }

        val output1 = PathOutput(fileOut1)

        assert(dao.findOutput(output1.path) === Some(output1.normalized))
        assert(dao.findFailedOutput(output1.path) === Some(output1.normalized))
        assert(dao.findHashedOutput(output1.path) === None)

        assert(dao.findExecution(output1).get.exitState.isFailure)
        assert(dao.findExecution(output1).get.outputs === Set(output1.normalized))
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
          val fileIn = store[TXT].from("src/test/resources/a.txt")
          val fileOut1 = store[TXT].to("$workDir/fileOut1.txt")
          val fileOut2 = store[TXT].to("$workDir/fileOut2.txt")
          cmd"asdfasdf $$fileIn $$fileOut1"
          cmd"asdfasdf $$fileOut1 $$fileOut2"
       */
      val script =
      s"""val fileIn = store[TXT].from(${SourceUtils.toStringLiteral(fileIn)})
            val fileOut1 = store[TXT].to(${SourceUtils.toStringLiteral(fileOut1)})
            val fileOut2 = store[TXT].to(${SourceUtils.toStringLiteral(fileOut2)})
            cmd"$bogusCommandName $$fileIn $$fileOut1"
            cmd"$bogusCommandName $$fileOut1 $$fileOut2""""

      //Run the script and validate the results
      def run(): Unit = {
        val (executable, results) = compileAndRun(script)

        val firstJob = jobThatWritesTo(executable)(fileOut1).get
        val secondJob = jobThatWritesTo(executable)(fileOut2).get

        {
          import Matchers._

          val exitCode = 127
          results(firstJob) shouldEqual CommandResult(exitCode)
          results.contains(secondJob) shouldBe false

          results(firstJob).isFailure shouldBe true

          results should have size 1
        }

        val output1 = PathOutput(fileOut1)
        val output2 = PathOutput(fileOut2)

        assert(dao.findExecution(output1).get.exitState.isFailure)
        assert(dao.findExecution(output1).get.outputs === Set(output1.normalized))

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
    val asyncChunkRunner = AsyncLocalChunkRunner()

    val mockRunner = MockChunkRunner(asyncChunkRunner)

    (resumptiveExecuter.copy(runner = mockRunner)(resumptiveExecuter.executionContext), mockRunner)
  }

  private def loamEngine = {
    val outMessageSink = LoggableOutMessageSink(this)

    val (executer, _) = makeLoggingExecuter

    LoamEngine(LoamCompiler(outMessageSink), executer, outMessageSink)
  }

  private def normalize(po: PathOutput): PathOutput = PathOutput(po.normalized.path)

  private def compileAndRun(script: String): (Executable, Map[LJob, JobState]) = {
    val engine = loamEngine

    val executable = engine.compileToExecutable(script).get

    val results = engine.executer.execute(executable)

    (executable, results)
  }

  private def allJobsFrom(executable: Executable): Seq[LJob] = ExecuterHelpers.flattenTree(executable.jobs).toSeq

  private def jobThatWritesTo(executable: Executable)(fileNameSuffix: Path): Option[LJob] = {
    val allJobs = allJobsFrom(executable)

    def outputMatches(o: Output): Boolean =
      o.asInstanceOf[Output.PathBased].path.toString.endsWith(fileNameSuffix.toString)

    def jobMatches(j: LJob): Boolean = j.outputs.exists(outputMatches)

    allJobs.find(jobMatches)
  }

  // scalastyle:on no.whitespace.before.left.bracket
}
