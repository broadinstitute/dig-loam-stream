package loamstream

import java.nio.file.Path
import java.nio.file.Paths

import scala.concurrent.ExecutionContext

import org.scalatest.FunSuite

import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef
import loamstream.conf.ExecutionConfig
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.loam.LoamGraph
import loamstream.loam.LoamCmdTool
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.DbBackedExecutionRecorder
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.Executable
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.execute.MockChunkRunner
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobStatus.Skipped
import loamstream.model.jobs.LJob
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.DataHandle.PathHandle
import loamstream.model.jobs.StoreRecord
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.Loggable
import loamstream.util.Sequence
import loamstream.model.jobs.PseudoExecution
import loamstream.model.jobs.JobOracle
import loamstream.model.execute.Run
import loamstream.conf.LsSettings
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult
import loamstream.model.execute.HashingStrategy
import scala.collection.compat._



/**
  * @author kaan
  * Sep 27, 2016
  */
final class ExecutionResumptionEndToEndTest extends FunSuite with ProvidesSlickLoamDao with Loggable { 

  // scalastyle:off no.whitespace.before.left.bracket

  private val currentRun: Run = Run.create()
  
  test("Jobs are skipped if their outputs were already produced by a previous run") {
    def copyAToB(fileIn: Path, fileOut: Path): LoamGraph = TestHelpers.makeGraph { implicit context =>
      import loamstream.loam.LoamSyntax._
       
      val in = LoamPredef.store(fileIn).asInput
      val out = LoamPredef.store(fileOut)
      
      cmd"cp $in $out".in(in).out(out)
    }
    
    def copyAToBToC(fileIn: Path, fileOut1: Path, fileOut2: Path): LoamGraph = {
      TestHelpers.makeGraph { implicit context => 
        import loamstream.loam.LoamSyntax._
      
        val in = LoamPredef.store(fileIn).asInput
        val out1 = LoamPredef.store(fileOut1)
        val out2 = LoamPredef.store(fileOut2)
        
        cmd"cp $in $out1".in(in).out(out1)
        cmd"cp $out1 $out2".in(out1).out(out2)
      }
    }
    
    //Run the script and validate the results
    def run(
        secondScript: LoamGraph,
        fileOut1: Path,
        fileOut2: Path)(expectedExecutions: Seq[Execution.Persisted]): Unit = {
      
      val (executable, executions) = compileAndRun(secondScript)

      val updatedOutput0 = StoreRecord(PathHandle(fileOut1))
      val updatedOutput1 = StoreRecord(PathHandle(fileOut2))

      //Jobs and results come back as an unordered map, so we need to find the jobs we're looking for. 
      val firstJob = jobThatWritesTo(executable)(fileOut1).get
      val secondJob = jobThatWritesTo(executable)(fileOut2).get

      val firstExecution = executions(firstJob)
      val secondExecution = executions(secondJob)
      
      assert(firstExecution.status === expectedExecutions(0).status)
      assert(firstExecution.result === expectedExecutions(0).result)
      
      assert(secondExecution.status === expectedExecutions(1).status)
      assert(secondExecution.result === expectedExecutions(1).result)
      
      assert(executions.size === 2)

      def testPersisted(output: StoreRecord, expected: Execution.Persisted): Unit = {
        val persistedExecution = findExecution(output).get
      
        assert(persistedExecution.status === expected.status)

        val expectedResult0: Option[JobResult] = {
          if(persistedExecution.status.isSkipped) Some(CommandResult(0)) else expected.result
        }
        
        assert(findExecution(output).get.result === expectedResult0)

        assert(persistedExecution.outputs === Set(output))
      }
      
      testPersisted(updatedOutput0, expectedExecutions(0))
      testPersisted(updatedOutput1, expectedExecutions(1))
    }        

    
    def doFirstPart(fileIn: Path, fileOut1: Path, fileOut2: Path): Unit = {
      val firstScript = copyAToB(fileIn, fileOut1) 
  
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

      val output1 = StoreRecord(PathHandle(fileOut1))
      val output2 = StoreRecord(PathHandle(fileOut2))

      val executionFromOutput1 = findExecution(output1).get
      
      val output1Result = executionFromOutput1.result.get.asInstanceOf[CommandResult]
      
      assert(output1Result.exitCode === 0)
      assert(executionFromOutput1.outputs === Set(output1))

      assert(findExecution(output2) === None)
    }
    
    def doSecondPart(fileIn: Path, fileOut1: Path, fileOut2: Path): Unit = {
      val secondScript = copyAToBToC(fileIn, fileOut1, fileOut2) 
  
      import TestHelpers.{ executionFrom, executionFromResult }
      
      def doRun(expectedStatuses: Seq[Execution.Persisted]): Unit = {
        run(secondScript, fileOut1, fileOut2)(expectedStatuses)
      }
      
      def skippedExecution = executionFrom(Skipped, result = None)
      def ranSuccessfullyExecution = executionFromResult(CommandResult(0))
      
      //Run the second script a few times.  The first time, we expect the first job to be skipped, and the second one
      //to be run.  We expect both jobs to be skipped in all subsequent runs.
      doRun(Seq(executionFrom(Skipped, result = None), ranSuccessfullyExecution))
      doRun(Seq(skippedExecution, skippedExecution))
      doRun(Seq(skippedExecution, skippedExecution))
      doRun(Seq(skippedExecution, skippedExecution))
    }
    
    val fileIn = Paths.get("src", "test", "resources", "a.txt")
  
    registerRunAndThen(currentRun) {
      withWorkDir { workDir =>
        val fileOut1 = workDir.resolve("fileOut1.txt")
        val fileOut2 = workDir.resolve("fileOut2.txt")
  
        doFirstPart(fileIn, fileOut1, fileOut2)
  
        doSecondPart(fileIn, fileOut1, fileOut2)
      }
    }
  }
  
  test("Single failed job's exit status is recorded properly") {
    val validInputFile = Paths.get("src", "test", "resources", "a.txt")
    
    val bogusCommandName = "asdfasdf"
    
    //Loam for a single invocation of a bogus command:
    def makeGraph(fileOut1: Path): LoamGraph = TestHelpers.makeGraph { implicit context =>
      import loamstream.loam.LoamSyntax._
      
      val in = LoamPredef.store(validInputFile).asInput
      val out1 = LoamPredef.store(fileOut1)
      
      cmd"$bogusCommandName $in $out1".in(in).out(out1)
    }
    
    //Run the script and validate the results
    def run(fileOut1: Path): Unit = {
      val (executable, executions) = compileAndRun(makeGraph(fileOut1))

      val allJobs = allJobsFrom(executable)

      val allCommandLines = allJobs.collect { case clj: CommandLineJob => clj.commandLineString }

      assert(allCommandLines.map(_.take(bogusCommandName.length)) === Seq(bogusCommandName))

      val onlyExecution = executions.values.head

      val onlyResultOpt = onlyExecution.result

      assert(onlyResultOpt.nonEmpty)

      val onlyResult = onlyResultOpt.get

      assert(executions.size === 1)
      assert(onlyExecution.resources.get.isInstanceOf[LocalResources] === true)
      assert(onlyResult.asInstanceOf[CommandResult].exitCode === 127)
      assert(onlyResult.isFailure === true)
      
      val output1 = StoreRecord(fileOut1)
      val recordOpt = dao.findStoreRecord(output1)

      assert(recordOpt === Some(output1))
      assert(recordOpt.get.hash.isEmpty)

      assert(findExecution(output1).get.result.get.isFailure)
      assert(findExecution(output1).get.outputs === Set(output1))
    }
    
    registerRunAndThen(currentRun) {
      withWorkDir { workDir => 
        val fileOut1 = workDir.resolve("fileOut1.txt")
  
        //Run the script a few times; we expect that the executer will try to run the bogus job every time, 
        //since it never succeeds.
        run(fileOut1)
        run(fileOut1)
        run(fileOut1)
      }
    }
  }

  test("Exit status of failed jobs are recorded properly") {
    val validInputFile = Paths.get("src", "test", "resources", "a.txt")
    
    val bogusCommandName = "asdfasdf"
    
    def makeGraph(fileOut1: Path, fileOut2: Path): LoamGraph = TestHelpers.makeGraph { implicit context =>
      import loamstream.loam.LoamSyntax._

      val in = LoamPredef.store(validInputFile).asInput
      val out1 = LoamPredef.store(fileOut1)
      val out2 = LoamPredef.store(fileOut2)
      
      cmd"$bogusCommandName $in $out1".in(in).out(out1)
      cmd"$bogusCommandName $out1 $out2".in(out1).out(out2)
    }
    
    //Run the script and validate the results
    def run(fileOut1: Path, fileOut2: Path): Unit = {
      val (executable, executions) = compileAndRun(makeGraph(fileOut1, fileOut2))

      val firstJob = jobThatWritesTo(executable)(fileOut1).get
      val secondJob = jobThatWritesTo(executable)(fileOut2).get

      assert(executions(firstJob).result.get.asInstanceOf[CommandResult].exitCode === 127)
        
      assert(executions(firstJob).resources.get.isInstanceOf[LocalResources] === true)
      assert(executions.contains(secondJob) === false)

      assert(executions(firstJob).result.get.isFailure === true)

      assert(executions.size === 1)

      val output1 = StoreRecord(fileOut1)
      val output2 = StoreRecord(fileOut2)

      assert(findExecution(output1).get.result.get.isFailure)
      assert(findExecution(output1).get.outputs === Set(output1))

      //NB: The job that referenced output2 didn't get run, so its execution should not have been recorded 
      assert(findExecution(output2) === None)
    }
    
    registerRunAndThen(currentRun) {
      withWorkDir { workDir =>
        val fileOut1 = workDir.resolve("fileOut1.txt")
        val fileOut2 = workDir.resolve("fileOut2.txt")
  
        //Run the script a few times; we expect that the executer will try to run both (bogus) jobs every time, 
        //since they never succeed.
        run(fileOut1, fileOut2)
        run(fileOut1, fileOut2)
        run(fileOut1, fileOut2)
      }
    }
  }

  private val sequence: Sequence[Int] = Sequence()

  private def makeWorkDir(): Path = TestHelpers.getWorkDir(getClass.getSimpleName)

  private def makeLoggingExecuter: (RxExecuter, MockChunkRunner) = {
    val asyncChunkRunner = AsyncLocalChunkRunner(ExecutionConfig.default)(ExecutionContext.global)

    val mockRunner = MockChunkRunner(asyncChunkRunner)

    val delegateExecuter: RxExecuter = {
      val executionRecorder = new DbBackedExecutionRecorder(dao, HashingStrategy.HashOutputs)
      
      RxExecuter.defaultWith(new DbBackedJobFilter(dao), executionRecorder)
    }
    
    val resultExecuter = {
      delegateExecuter.copy(runner = mockRunner, maxRunsPerJob = 1)(delegateExecuter.executionContext)
    }
    
    (resultExecuter, mockRunner)
  }

  private def loamEngine = {
    val (executer, _) = makeLoggingExecuter

    LoamEngine(TestHelpers.config, LsSettings.noCliConfig, LoamCompiler.default, executer)
  }

  private def compileAndRun(graph: LoamGraph): (Executable, Map[LJob, Execution]) = {
    val engine = loamEngine

    val executable = LoamEngine.toExecutable(graph)

    
    
    val executions = engine.executer.execute(executable, JobOracle.fromExecutable(engine.config.executionConfig, _))

    (executable, executions)
  }

  private def allJobsFrom(executable: Executable): Seq[JobNode] = {
    ExecuterHelpers.flattenTree(executable.jobNodes).to(Seq)
  }

  private def jobThatWritesTo(executable: Executable)(fileNameSuffix: Path): Option[LJob] = {
    val allJobs = allJobsFrom(executable).map(_.job)

    def outputMatches(o: DataHandle): Boolean = {
      o.asInstanceOf[DataHandle.PathHandle].path.toString.endsWith(fileNameSuffix.toString)
    }

    def jobMatches(j: LJob): Boolean = j.outputs.exists(outputMatches)

    allJobs.find(jobMatches)
  }
  
  private def withWorkDir[A](body: Path => A): A = TestHelpers.withWorkDir(getClass.getSimpleName)(body)

  // scalastyle:on no.whitespace.before.left.bracket
}
