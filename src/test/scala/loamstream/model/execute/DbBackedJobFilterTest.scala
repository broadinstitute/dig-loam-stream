package loamstream.model.execute

import java.nio.file.Paths
import java.time.Instant

import org.scalatest.FunSuite
import org.scalatest.PrivateMethodTester

import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.OutputRecord
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.HashType.Sha1
import loamstream.util.PathUtils
import loamstream.model.jobs.OutputStreams
import loamstream.TestHelpers
import loamstream.model.jobs.LJob
import java.nio.file.Path

/**
 * @author clint
 *         date: Sep 30, 2016
 */
final class DbBackedJobFilterTest extends FunSuite with ProvidesSlickLoamDao
  with PrivateMethodTester with ProvidesEnvAndResources {

  import TestHelpers.path
  
  private val nonexistentPath = path("non/existent/blah.txt")

  private trait Outputs {
    def p0: Path
    def p1: Path
    def p2: Path
    
    def o0: Output.PathOutput
    def o1: Output.PathOutput
    def o2: Output.PathOutput
    def nonExistentOutput: Output.PathOutput
    
    final lazy val cachedOutput0: OutputRecord = o0.toOutputRecord
    final lazy val cachedOutput1: OutputRecord = o1.toOutputRecord
    final lazy val cachedOutput2: OutputRecord = o2.toOutputRecord
    final lazy val cachedNonExistentOutput: OutputRecord = nonExistentOutput.toOutputRecord
    
    final lazy val failedOutput0: OutputRecord = failedOutput(o0.pathInHost)
    final lazy val failedOutput1: OutputRecord = failedOutput(o1.pathInHost)
    final lazy val failedOutput2: OutputRecord = failedOutput(o2.pathInHost)
  }
  
  private object SimpleOutputs extends Outputs {
    override val p0 = path("src/test/resources/for-hashing/foo.txt")
    override val p1 = path("src/test/resources/for-hashing/empty.txt")
    override val p2 = path("src/test/resources/for-hashing/subdir/bar.txt")
    
    override val o0 = Output.PathOutput(p0)
    override val o1 = Output.PathOutput(p1)
    override val o2 = Output.PathOutput(p2)
    override val nonExistentOutput = Output.PathOutput(nonexistentPath)
  }
  
  private def testWithSimpleOutputSet(name: String)(body: Outputs => Any): Unit = {
    test(name) {
      body(SimpleOutputs)
    }
  }

  private def executions = dao.allExecutions.toSet

  import loamstream.model.jobs.JobResult._
  import TestHelpers.dummyOutputStreams

  test("sanity check paths") {
    import java.nio.file.Files.exists
    import SimpleOutputs._
    
    assert(exists(p0))
    assert(exists(p1))
    assert(exists(p2))
    assert(exists(nonexistentPath) === false)
    
    assert(o0.isMissing === false)
    assert(o1.isMissing === false)
    assert(o2.isMissing === false)
    assert(nonExistentOutput.isMissing)
    
    assert(cachedOutput0.isMissing === false)
    assert(cachedOutput1.isMissing === false)
    assert(cachedOutput2.isMissing === false)
    assert(cachedNonExistentOutput.isMissing)
  }
  
  test("record() - no Executions") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      filter.record(Nil)

      assert(executions === Set.empty)
    }
  }

  test("record() - non-command-Execution") {
    def doTest(
        command: Option[String], 
        status: JobStatus, 
        result: Option[JobResult] = None): Unit = {
      createTablesAndThen {
        val filter = new DbBackedJobFilter(dao)
  
        assert(executions === Set.empty)
  
        val e = Execution(
            env = mockEnv, 
            cmd = command, 
            status = status, 
            result = result, 
            resources = Option(mockResources),
            outputStreams = None,
            outputs = Set.empty[OutputRecord])

        assert(e.isCommandExecution === false)
        
        filter.record(Seq(e))
  
        assert(executions === Set.empty)
      }
    }
    
    doTest(None, JobStatus.Succeeded, Option(CommandResult(0)))
    doTest(None, JobStatus.Failed, Option(JobResult.Failure))
    doTest(None, JobStatus.Succeeded)
    doTest(Some(mockCmd), JobStatus.Failed)
    doTest(Some(mockCmd), JobStatus.Succeeded)
  }

  test("record() - successful command-Execution, no outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(0)

      assert(cr.isSuccess)

      val e = Execution(
          env = mockEnv, 
          cmd = Option(mockCmd), 
          status = cr.toJobStatus, 
          result = Option(cr), 
          resources = Option(mockResources),
          outputStreams = Some(dummyOutputStreams),
          outputs = Set.empty[OutputRecord])

      filter.record(Seq(e))

      assertEqualFieldsFor(executions, Set(e))
    }
  }

  test("record() - failed command-Execution, no outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(42)

      assert(cr.isFailure)

      val e = Execution(
          env = mockEnv, 
          cmd = Option(mockCmd), 
          status = cr.toJobStatus, 
          result = Option(cr),
          resources = Option(mockResources),
          outputStreams = Some(dummyOutputStreams),
          outputs = Set.empty[OutputRecord])

      filter.record(Seq(e))

      assertEqualFieldsFor(executions, Set(e))
    }
  }

  testWithSimpleOutputSet("record() - successful command-Execution, some outputs") { outputs =>
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
  
      assert(executions === Set.empty)
  
      val cr = CommandResult(0)
  
      assert(cr.isSuccess)
  
      import outputs.{o0, o1, o2, cachedOutput0, cachedOutput1, cachedOutput2 }
      
      val e = Execution.fromOutputs(mockEnv, mockCmd, cr, dummyOutputStreams, Set(o0, o1, o2))
      
      val withHashedOutputs = e.withOutputRecords(Set(cachedOutput0, cachedOutput1, cachedOutput2))
  
      filter.record(Seq(e))
  
      assertEqualFieldsFor(executions, Set(withHashedOutputs))
    }
  }

  testWithSimpleOutputSet("record() - failed command-Execution, some outputs") { outputs =>
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(42)

      assert(cr.isFailure)

      import outputs.{o0, o1, o2, failedOutput0, failedOutput1, failedOutput2 }
      
      val e = Execution.fromOutputs(mockEnv, mockCmd, cr, dummyOutputStreams, Set(o0, o1, o2))

      filter.record(Seq(e))

      val expected = Set(
          Execution(
              env = mockEnv, 
              cmd = mockCmd, 
              result = CommandResult(42),
              outputStreams = e.outputStreams.get,
              outputs = failedOutput0, failedOutput1, failedOutput2))
      
      assertEqualFieldsFor(executions, expected)
    }
  }

  testWithSimpleOutputSet("shouldRun - failed and successful runs") { outputs =>
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao, HashingStrategy.HashOutputs)
        
      val jobName = "dummyJob"
      
      assert(executions === Set.empty)

      val failedCommandLine = mockCmd
      val successfulCommandLine = s"${mockCmd}asdfasdf"
      
      def commandLineJob(commandLine: String, outputs: Set[Output]) = {
        CommandLineJob(commandLine, TestHelpers.path("."), Environment.Local, outputs = outputs)
      }
      
      val successfulJob = commandLineJob(successfulCommandLine, Set(outputs.o0))
      val failedJob = commandLineJob(failedCommandLine, Set(outputs.o0))
      
      {
        val failure = CommandResult(42)
        assert(failure.isFailure)
  
        val success = CommandResult(0)
        assert(success.isSuccess)
        
        import TestHelpers.{ dummyOutputStreams => outputStreams }
        
        val failedExec = Execution.fromOutputs(mockEnv, failedCommandLine, failure, outputStreams, failedJob.outputs)
        
        val successfulExec = {
          Execution.fromOutputs(mockEnv, successfulCommandLine, success, outputStreams, successfulJob.outputs)
        }
  
        filter.record(Seq(failedExec, successfulExec))
      }
      
      //Doesn't need to be re-run
      assert(filter.shouldRun(successfulJob) === false)
      
      //Should run because a job with the same command line failed "last time"
      assert(filter.shouldRun(failedJob))
    }
  }
  
  testWithSimpleOutputSet("needsToBeRun/hasDifferentHash/hasDifferentModTime - should hash") { outputs =>
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao, HashingStrategy.HashOutputs)
      
      val jobName = "dummyJob"

      assert(executions === Set.empty)

      import outputs.{o0, o1, o2, nonExistentOutput, cachedNonExistentOutput, cachedOutput0, cachedOutput1 }
      
      {
        val failure = CommandResult(42)
        assert(failure.isFailure)
  
        val success = CommandResult(0)
        assert(success.isSuccess)
        
        val failedExec = Execution.fromOutputs(mockEnv, mockCmd, failure, dummyOutputStreams, Set(o0))
        
        val successfulExec = {
          Execution.fromOutputs(mockEnv, mockCmd, success, dummyOutputStreams, Set(o1, nonExistentOutput))
        }
  
        filter.record(Seq(failedExec, successfulExec))
      }

      // Missing record:  'hasDifferentHash' --> false
      //                  'hasDifferentModTime --> false
      //                  'needsToBeRun' --> true
      assert(cachedNonExistentOutput.isMissing)
      assert(filter.hasDifferentHash(cachedNonExistentOutput) === false)
      assert(filter.hasDifferentModTime(cachedNonExistentOutput) === false)
      assert(filter.needsToBeRun(jobName, cachedNonExistentOutput))

      // Older record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'hasDifferentModTime --> true
      //                                                'needsToBeRun' --> true
      {
        val olderRec = cachedOutput1.withLastModified(Instant.ofEpochMilli(0))
        assert(filter.hasDifferentHash(olderRec) === false)
        assert(filter.hasDifferentModTime(olderRec))
        assert(filter.needsToBeRun(jobName, olderRec))
      }
      
      // Newer record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'hasDifferentModTime --> true
      //                                                'needsToBeRun' --> true
      {
        val newerRec = {
          val modTimePlusOne = Instant.ofEpochMilli(1 + cachedOutput1.lastModified.get.toEpochMilli)
          
          cachedOutput1.withLastModified(modTimePlusOne)
        }
        assert(filter.hasDifferentHash(newerRec) === false)
        assert(filter.hasDifferentModTime(newerRec))
        assert(filter.needsToBeRun(jobName, newerRec))
      }
      
      // Same record (as is matching record in DB): 'hasDifferentHash' --> false
      //                                            'hasDifferentModTime --> false
      //                                            'needsToBeRun' --> true
      assert(filter.hasDifferentHash(cachedOutput1) === false)
      assert(filter.hasDifferentModTime(cachedOutput1) === false)
      assert(filter.needsToBeRun(jobName, cachedOutput1) === false)

      // Unhashed record: 'needsToBeRun' --> true
      assert(filter.needsToBeRun(jobName, cachedOutput0))

      // Record with different hash:  'hasDifferentHash' --> true
      //                              'needsToBeRun' --> true
      val recWithDiffHash = OutputRecord( cachedOutput1.loc,
                                          Option("bogus-hash"),
                                          Option(Sha1.algorithmName),
                                          cachedOutput1.lastModified)
                                          
      assert(filter.hasDifferentHash(recWithDiffHash))
      assert(filter.needsToBeRun(jobName, recWithDiffHash))
      assert(filter.hasDifferentModTime(recWithDiffHash) === false)

      // Otherwise: 'needsToBeRun' --> false
      assert(filter.needsToBeRun(jobName, cachedOutput1) === false)
    }
  }
  
  testWithSimpleOutputSet("needsToBeRun/hasDifferentHash/hasDifferentModTime - hashing disabled") { outputs =>
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao, HashingStrategy.DontHashOutputs)
      val jobName = "dummyJob"

      assert(executions === Set.empty)

      import outputs.{o0, o1, o2, nonExistentOutput, cachedNonExistentOutput, cachedOutput0, cachedOutput1 }
      
      {
        val failure = CommandResult(42)
        assert(failure.isFailure)
  
        val success = CommandResult(0)
        assert(success.isSuccess)

        val failedExec = Execution.fromOutputs(mockEnv, mockCmd, failure, dummyOutputStreams, Set(o0))
        
        val successfulExec = {
          Execution.fromOutputs(mockEnv, mockCmd, success, dummyOutputStreams, Set(o1, nonExistentOutput))
        }
  
        filter.record(Seq(failedExec, successfulExec))
      }

      // Missing record:  'hasDifferentHash' --> false
      //                  'hasDifferentModTime --> false
      //                  'needsToBeRun' --> true
      assert(cachedNonExistentOutput.isMissing)
      assert(filter.hasDifferentHash(cachedNonExistentOutput) === false)
      assert(filter.hasDifferentModTime(cachedNonExistentOutput) === false)
      assert(filter.needsToBeRun(jobName, cachedNonExistentOutput))

      // Older record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'hasDifferentModTime --> true
      //                                                'needsToBeRun' --> true
      {
        val olderRec = cachedOutput1.withLastModified(Instant.ofEpochMilli(0))

        assert(filter.hasDifferentHash(olderRec) === false)
        assert(filter.hasDifferentModTime(olderRec))
        assert(filter.needsToBeRun(jobName, olderRec))
      }
      
      // Newer record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'hasDifferentModTime --> true
      //                                                'needsToBeRun' --> true
      {
        val newerRec = {
          val modTimePlusOne = Instant.ofEpochMilli(1 + cachedOutput1.lastModified.get.toEpochMilli)
          
          cachedOutput1.withLastModified(modTimePlusOne)
        }
        assert(filter.hasDifferentHash(newerRec) === false)
        assert(filter.hasDifferentModTime(newerRec))
        assert(filter.needsToBeRun(jobName, newerRec))
      }
      
      // Same record (as is matching record in DB): 'hasDifferentHash' --> false
      //                                            'hasDifferentModTime --> false
      //                                            'needsToBeRun' --> true
      assert(filter.hasDifferentHash(cachedOutput1) === false)
      assert(filter.hasDifferentModTime(cachedOutput1) === false)
      assert(filter.needsToBeRun(jobName, cachedOutput1) === false)

      // Unhashed record: 'needsToBeRun' --> false, since hashes aren't considered
      assert(filter.needsToBeRun(jobName, cachedOutput0) === false)

      // Record with different hash:  'hasDifferentHash' --> true
      //                              'needsToBeRun' --> true
      val recWithDiffHash = OutputRecord( cachedOutput1.loc,
                                          Option("bogus-hash"),
                                          Option(Sha1.algorithmName),
                                          cachedOutput1.lastModified)
      assert(filter.hasDifferentHash(recWithDiffHash))
      assert(filter.hasDifferentModTime(recWithDiffHash) === false)
      //since hashes aren't considered
      assert(filter.needsToBeRun(jobName, recWithDiffHash) === false)

      // Otherwise: 'needsToBeRun' --> false
      assert(filter.needsToBeRun(jobName, cachedOutput1) === false)
    }
  }

  testWithSimpleOutputSet("command string comparison") { outputSet =>
    def cmdLineJob(cmd: String, outputs: Set[Output]): CommandLineJob = {
      CommandLineJob(
        commandLineString = cmd,
        workDir = PathUtils.getCurrentDirectory,
        executionEnvironment = mockEnv,
        outputs = outputs)
    }

    def mockJob(cmd: String, outputs: Set[Output]): MockJob = {
      new MockJob.FromJobFn(
        toReturnFn = mockRunData(_),
        name = "mock job",
        inputs = Set.empty,
        outputs = outputs,
        delay = 0)
    }

    def execution(cmd: String, outputs: Set[Output]): Execution = {
      Execution.fromOutputs(mockEnv, cmd, CommandResult(0), dummyOutputStreams, outputs)
    }

    val cmd0 = "cmd0"
    val cmd1 = "cmd1"
    
    import outputSet.{o0, o1, o2 }
    
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      val outputs: Set[Output] = Set(o0, o1)

      filter.record(Iterable(execution(cmd0, outputs)))

      assert(filter.findCommandLineInDb(o0.location) === Some(cmd0))
      assert(filter.findCommandLineInDb(o1.location) === Some(cmd0))

      val cmdLineJob0 = cmdLineJob(cmd0, outputs)

      assert(filter.hasNewCommandLine(cmdLineJob0) === false)

      filter.record(Iterable(execution(cmd1, Set[Output](o2))))
      
      val cmdLineJob1 = cmdLineJob("cmd1-altered", Set(o2))

      assert(filter.hasNewCommandLine(cmdLineJob1) === true)

      // Non-CommandLineJob's shouldn't be affected by cmd string checks
      val nonCmdLineJob0 = mockJob(cmd0, outputs)

      assert(filter.hasNewCommandLine(nonCmdLineJob0) === false)

      val nonCmdLineJob1 = mockJob("cmd1-altered", Set(o2))

      assert(filter.hasNewCommandLine(nonCmdLineJob1) === false)
    }
  }
}
