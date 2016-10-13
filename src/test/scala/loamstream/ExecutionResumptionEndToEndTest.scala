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
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.JobState
import loamstream.util.Hit
import loamstream.util.Loggable
import loamstream.util.Sequence
import loamstream.util.Shot
import loamstream.model.jobs.Output
import loamstream.model.jobs.JobState.CommandResult
import loamstream.model.jobs.JobState.FailedWithException
import loamstream.model.jobs.LJob
import loamstream.model.execute.MockChunkRunner
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.jobs.commandline.CommandLineJob

/**
  * @author kaan
  *         date: Sep 27, 2016
  */
final class ExecutionResumptionEndToEndTest extends FunSuite with ProvidesSlickLoamDao with Loggable {

  private val resumptiveExecuter = RxExecuter.defaultWith(new DbBackedJobFilter(dao))

  private val outMessageSink = LoggableOutMessageSink(this)

  private val loamEngine = LoamEngine(LoamCompiler(outMessageSink), resumptiveExecuter, outMessageSink)
  
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

      val firstCompiled = loamEngine.run(firstScript)

      val firstResults = for {
        (job, result) <- firstCompiled.jobResultsOpt.get
      } yield result

      val firstResultValues = firstResults.toIndexedSeq

      {
        import Matchers._
        
        val cr @ CommandResult(_) = firstResultValues(0)
        
        cr.isSuccess shouldBe true
        
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
  
        val secondCompiled = loamEngine.run(secondScript)
  
        val secondResults = for {
          (job, result) <- secondCompiled.jobResultsOpt.get
        } yield result
  
        val secondResultValues = secondResults.toIndexedSeq
  
        secondResultValues should have size 2
        secondResultValues(0) shouldBe JobState.Skipped
        secondResultValues(1) shouldBe a [JobState.CommandResult]
        secondResultValues(1).isSuccess shouldBe true

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
      
      def log(s: String): Unit = println(s"%%%%%%%%%%%% $s")
      
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

      val executable = loamEngine.compileToExecutable(script).get
        
/*      println(s"%%%%%%%%%%%% Jobs from second Executable (${executable.jobs.size}): ")
      executable.jobs.foreach { job =>
        println(s"%%%%%%%%%%%% $job => ${job.outputs}")
      }
      
      println(s"%%%%%%%%%%%% Trees from second Executable (${executable.jobs.size}): ")
      executable.jobs.foreach(_.print(doPrint = log))*/
            
      val (executer, mockRunner) = {
        val asyncChunkRunner = RxExecuter.AsyncLocalChunkRunner
        
        val mockRunner = MockChunkRunner(asyncChunkRunner, asyncChunkRunner.maxNumJobs)
        
        (RxExecuter(mockRunner)(scala.concurrent.ExecutionContext.global), mockRunner)
      }
      
      val jobStates: Map[LJob, JobState] = executer.execute(executable)

      val allJobs = ExecuterHelpers.flattenTree(executable.jobs).toSeq
      
      val allCommandLines = allJobs.collect { case clj: CommandLineJob => clj.commandLineString }
      
      assert(allCommandLines.map(_.take(bogusCommandName.size)) === Seq(bogusCommandName))
      
      val f0 @ FailedWithException(_) = jobStates.values.head

      {
        import Matchers._
          
        f0.isFailure shouldBe true
  
        jobStates should have size 1
      }
        
      val output1 = Output.PathOutput(Paths.get(fileOut1))
      
      println(s"All Outputs: ${dao.allOutputs}")
      println(s"All Executions: ${dao.allExecutions}")
      assert(dao.findOutput(output1.path).get === output1)
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
            cmd"${bogusCommandName}2222 $$fileOut1 $$fileOut2""""

      val secondExecutable = loamEngine.compileToExecutable(secondScript).get
        
      println(s"%%%%%%%%%%%% Jobs from second Executable (${secondExecutable.jobs.size}): ")
      secondExecutable.jobs.foreach { job =>
        println(s"%%%%%%%%%%%% $job => ${job.outputs}")
      }
      
      println(s"%%%%%%%%%%%% Trees from second Executable (${secondExecutable.jobs.size}): ")
      secondExecutable.jobs.foreach(_.print(doPrint = log))
            
      val secondCompiled = loamEngine.run(secondScript)

      val secondResults = for {
        (job, result) <- secondCompiled.jobResultsOpt.get
      } yield result

      val secondResultValues = secondResults.toIndexedSeq

      val f0 @ FailedWithException(_) = secondResultValues(0)
      val f1 @ FailedWithException(_) = secondResultValues(1)

      {
        import Matchers._
          
        f0.isFailure shouldBe true
        f1.isFailure shouldBe true
  
        secondResultValues should have size 2
      }
        
      val output1 = Output.PathOutput(Paths.get(fileOut1))
      val output2 = Output.PathOutput(Paths.get(fileOut2))
      
      assert(dao.findExecution(output1).get.exitState === 0)
      assert(dao.findExecution(output2).get.exitState === 0)
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
}
