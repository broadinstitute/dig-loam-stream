package loamstream

import java.nio.file.Path
import java.nio.file.Paths

import org.scalatest.FunSuite
import org.scalatest.Matchers

import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.Executable
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.execute.MockChunkRunner
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.JobState
import loamstream.model.jobs.JobState.CommandInvocationFailure
import loamstream.model.jobs.JobState.CommandResult
import loamstream.model.jobs.JobState.FailedWithException
import loamstream.model.jobs.JobState.Skipped
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.Output.PathOutput
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.Loggable
import loamstream.util.PathUtils
import loamstream.util.Sequence
import loamstream.util.Shot

/**
  * @author kaan
  *         date: Sep 27, 2016
  */
final class ExecutionResumptionEndToEndTest extends FunSuite with ProvidesSlickLoamDao with Loggable {

  //TODO: These tests won't run on Windows, since they need cp
  
  test("Jobs are skipped if their outputs were already produced by a previous run") {
    // scalastyle:off no.whitespace.before.left.bracket

    createTablesAndThen {
      val workDir = makeWorkDir()

      val workDirInLoam = workDir.toString.replace("\\", "/")

      val fileOut1 = s"$workDirInLoam/fileOut1.txt"
      val fileOut2 = s"$workDirInLoam/fileOut2.txt"
      
      /* Loam for the first run that mimics an incomplete pipeline run:
          val fileIn = store[String].from("src/test/resources/a.txt")
          val fileOut1 = store[String].to("$workDir/fileOut1.txt")
          cmd"cp $$fileIn $$fileOut1
       */
      val firstScript =
        s"""val fileIn = store[String].from("src/test/resources/a.txt")
            val fileOut1 = store[String].to("$fileOut1")
            cmd"cp $$fileIn $$fileOut1""""

      val (_, firstResults) = compileAndRun(firstScript)
            
      val firstResultValues = firstResults.values.toIndexedSeq

      {
        import Matchers._
        
        firstResultValues(0) shouldBe a [CommandResult]
        firstResultValues(0).isSuccess shouldBe true
        
        firstResultValues should have size 1
      
        /* Loam for the second run that mimics a run launched subsequently to an incomplete first run:
            val fileIn = store[String].from("src/test/resources/a.txt")
            val fileOut1 = store[String].to("$workDir/fileOut1.txt")
            val fileOut2 = store[String].to("$workDir/fileOut2.txt")
            cmd"cp $$fileIn $$fileOut1"
            cmd"cp $$fileOut1 $$fileOut2
         */
        val secondScript =
          s"""val fileIn = store[String].from("src/test/resources/a.txt")
              val fileOut1 = store[String].to("$fileOut1")
              val fileOut2 = store[String].to("$fileOut2")
              cmd"cp $$fileIn $$fileOut1"
              cmd"cp $$fileOut1 $$fileOut2""""
  
        val (secondExecutable, secondResults) = compileAndRun(secondScript)
  
        //Jobs and results come back as an unordered map, so we need to find the jobs we're looking for. 
        val firstJob = jobThatWritesTo(secondExecutable)(fileOut1).get
        val secondJob = jobThatWritesTo(secondExecutable)(fileOut2).get
        
        secondResults(firstJob) shouldBe Skipped
        secondResults(secondJob) shouldBe a [CommandResult]
        secondResults(secondJob).isSuccess shouldBe true
        
        secondResults should have size 2

        // scalastyle:off no.whitespace.before.left.bracket
      }
      
      val output1 = Output.PathOutput(Paths.get(fileOut1))
      val output2 = Output.PathOutput(Paths.get(fileOut2))
      
      assert(dao.findExecution(output1).get.exitState === CommandResult(0))
      assert(dao.findExecution(output2).get.exitState === CommandResult(0))
    }
  }
  
  test("Single failed job's exit status is recorded properly") {
    createTablesAndThen {
      val workDir = makeWorkDir()

      val workDirInLoam = workDir.toString.replace("\\", "/")

      val fileOut1 = s"$workDirInLoam/fileOut1.txt"
      
      val bogusCommandName = "asdfasdf"
      
      /* Loam for the second run that mimics a run launched subsequently to an incomplete first run:
          val fileIn = store[String].from("src/test/resources/a.txt")
          val fileOut1 = store[String].to("$workDir/fileOut1.txt")
          cmd"asdfasdf $$fileIn $$fileOut1"
       */
      val script = {
        s"""val fileIn = store[String].from("src/test/resources/a.txt")
            val fileOut1 = store[String].to("$fileOut1")
            cmd"$bogusCommandName $$fileIn $$fileOut1""""
      }

      val (executable, jobStates) = compileAndRun(script)

      val allJobs = allJobsFrom(executable)
      
      val allCommandLines = allJobs.collect { case clj: CommandLineJob => clj.commandLineString }
      
      assert(allCommandLines.map(_.take(bogusCommandName.size)) === Seq(bogusCommandName))

      {
        import Matchers._
       
        val onlyResult = jobStates.values.head
        
        onlyResult shouldBe a [CommandInvocationFailure]
        onlyResult.isFailure shouldBe true
  
        jobStates should have size 1
      }
        
      val output1 = Output.PathOutput(Paths.get(fileOut1))
      
      assert(dao.findOutput(output1.path) === Some(normalize(output1)))
      assert(dao.findFailedOutput(output1.path) === Some(normalize(output1)))
      assert(dao.findHashedOutput(output1.path) === None)
      assert(dao.findExecution(output1).get.exitState !== 0)
    }
  }
  
  test("Exit status of failed jobs are recorded properly") {
    createTablesAndThen {
      val workDir = makeWorkDir()

      val workDirInLoam = workDir.toString.replace("\\", "/")

      val fileOut1 = s"$workDirInLoam/fileOut1.txt"
      val fileOut2 = s"$workDirInLoam/fileOut2.txt"
      
      val bogusCommandName = "asdfasdf"
      
      def log(s: String): Unit = println(s"%%%%%%%%%%%% $s")
      
      /* Loam for the second run that mimics a run launched subsequently to an incomplete first run:
          val fileIn = store[String].from("src/test/resources/a.txt")
          val fileOut1 = store[String].to("$workDir/fileOut1.txt")
          val fileOut2 = store[String].to("$workDir/fileOut2.txt")
          cmd"asdfasdf $$fileIn $$fileOut1"
          cmd"asdfasdf $$fileOut1 $$fileOut2"
       */
      val secondScript =
        s"""val fileIn = store[String].from("src/test/resources/a.txt")
            val fileOut1 = store[String].to("$fileOut1")
            val fileOut2 = store[String].to("$fileOut2")
            cmd"$bogusCommandName $$fileIn $$fileOut1"
            cmd"$bogusCommandName $$fileOut1 $$fileOut2""""

      val (secondExecutable, secondResults) = compileAndRun(secondScript)

      val firstJob = jobThatWritesTo(secondExecutable)(fileOut1).get
      val secondJob = jobThatWritesTo(secondExecutable)(fileOut2).get

      {
        import Matchers._

        secondResults(firstJob) shouldBe a [CommandInvocationFailure]
        secondResults.contains(secondJob) shouldBe false
        
        secondResults(firstJob).isFailure shouldBe true
  
        secondResults should have size 1
      }
        
      val output1 = PathOutput(Paths.get(fileOut1))
      val output2 = PathOutput(Paths.get(fileOut2))
      
      assert(dao.findExecution(output1).get.exitState.isFailure)
      
      //NB: The job that referenced output2 didn't get run, so its execution should not have been recorded 
      assert(dao.findExecution(output2) === None)
    }
  }

  private val sequence: Sequence[Int] = Sequence()

  private def makeWorkDir(): Path = {
    def exists(path: Path): Boolean = path.toFile.exists

    val suffixes = sequence.iterator

    val candidates = suffixes.map(i => Paths.get(s"target/resumptive-executer-test$i"))

    val result = candidates.dropWhile(exists).next()

    val asFile = result.toFile

    asFile.mkdir()

    assert(asFile.exists)

    result
  }
  
  private val dbBackedJobFilter = new DbBackedJobFilter(dao)
  
  private val resumptiveExecuter = RxExecuter.defaultWith(dbBackedJobFilter)

  private val outMessageSink = LoggableOutMessageSink(this)

  private val loamEngine = LoamEngine(LoamCompiler(outMessageSink), resumptiveExecuter, outMessageSink)
  
  private def makeLoggingExecuter: (RxExecuter, MockChunkRunner) = {
    val asyncChunkRunner = RxExecuter.AsyncLocalChunkRunner
        
    val mockRunner = MockChunkRunner(asyncChunkRunner, asyncChunkRunner.maxNumJobs)
        
    (resumptiveExecuter.copy(runner = mockRunner)(resumptiveExecuter.executionContext), mockRunner)
  }
  
  private def normalize(po: PathOutput): PathOutput = PathOutput(PathUtils.normalizePath(po.path))
  
  private def compileAndRun(script: String): (Executable, Map[LJob, JobState]) = {
    val (executer, _) = makeLoggingExecuter
    
    val executable = loamEngine.compileToExecutable(script).get
    
    val results = executer.execute(executable)
    
    (executable, results)
  }
  
  private def allJobsFrom(executable: Executable): Seq[LJob] = ExecuterHelpers.flattenTree(executable.jobs).toSeq
  
  private def jobThatWritesTo(executable: Executable)(fileNameSuffix: String): Option[LJob] = {
    val allJobs = allJobsFrom(executable)
    
    def outputMatches(o: Output): Boolean = o.asInstanceOf[Output.PathBased].path.toString.endsWith(fileNameSuffix) 
    
    def jobMatches(j: LJob): Boolean = j.outputs.exists(outputMatches)
    
    allJobs.find(jobMatches)
  }
}
