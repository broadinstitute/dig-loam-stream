package loamstream.model.execute

import java.nio.file.Paths
import java.time.Instant

import org.scalatest.{FunSuite, PrivateMethodTester}
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.commandline.{CommandLineJob, CommandLineStringJob}
import loamstream.model.jobs.{Execution, JobResult, JobStatus, MockJob, Output, OutputRecord}
import loamstream.util.HashType.Sha1
import loamstream.util.PathUtils

/**
 * @author clint
 *         date: Sep 30, 2016
 */
final class DbBackedJobFilterTest extends FunSuite with ProvidesSlickLoamDao
  with PrivateMethodTester with ProvidesEnvAndResources {

  private val p0 = Paths.get("src/test/resources/for-hashing/foo.txt")
  private val p1 = Paths.get("src/test/resources/for-hashing/empty.txt")
  private val p2 = Paths.get("src/test/resources/for-hashing/subdir/bar.txt")
  private val p3 = Paths.get("non/existent/blah.txt")

  private val o0 = Output.PathOutput(p0)
  private val o1 = Output.PathOutput(p1)
  private val o2 = Output.PathOutput(p2)
  private val o3 = Output.PathOutput(p3)

  private val cachedOutput0 = o0.toOutputRecord
  private val cachedOutput1 = o1.toOutputRecord
  private val cachedOutput2 = o2.toOutputRecord
  private val cachedOutput3 = o3.toOutputRecord

  private val failedOutput0 = failedOutput(p0)
  private val failedOutput1 = failedOutput(p1)
  private val failedOutput2 = failedOutput(p2)

  private def executions = dao.allExecutions.toSet

  import JobResult._

  test("record() - no Executions") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      filter.record(Nil)

      assert(executions === Set.empty)
    }
  }

  test("record() - non-command-Execution") {
    def doTest(command: Option[String], status: JobStatus, result: Option[JobResult] = None): Unit = {
      createTablesAndThen {
        val filter = new DbBackedJobFilter(dao)
  
        assert(executions === Set.empty)
  
        val e = Execution(id = None, mockEnv, command, mockSettings, status, result,
          Option(mockResources), Set.empty[OutputRecord])

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

      val e = Execution(id = None, mockEnv, Option(mockCmd), mockSettings, cr.toJobStatus, Option(cr),
        Option(mockResources), Set.empty[OutputRecord])

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

      val e = Execution(id = None, mockEnv, Option(mockCmd), mockSettings, cr.toJobStatus, Option(cr),
        Option(mockResources), Set.empty[OutputRecord])

      filter.record(Seq(e))

      assertEqualFieldsFor(executions, Set(e))
    }
  }

  test("record() - successful command-Execution, some outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(0)

      assert(cr.isSuccess)

      val e = Execution.fromOutputs(mockEnv, mockCmd, mockSettings, cr, Set[Output](o0, o1, o2))
      val withHashedOutputs = e.withOutputRecords(Set(cachedOutput0, cachedOutput1, cachedOutput2))

      filter.record(Seq(e))

      assertEqualFieldsFor(executions, Set(withHashedOutputs))
    }
  }

  test("record() - failed command-Execution, some outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(42)

      assert(cr.isFailure)

      val e = Execution.fromOutputs(mockEnv, mockCmd, mockSettings, cr, Set[Output](o0, o1, o2))

      filter.record(Seq(e))

      val expected = Set(
          Execution(mockEnv, mockCmd, mockSettings, CommandResult(42),
                    failedOutput0, failedOutput1, failedOutput2))
      
      assertEqualFieldsFor(executions, expected)
    }
  }

  test("needsToBeRun/hasDifferentHash/isOlder") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      val jobName = "dummyJob"

      assert(executions === Set.empty)

      val failure = CommandResult(42)
      assert(failure.isFailure)

      val success = CommandResult(0)
      assert(success.isSuccess)

      val failedExecs = Execution.fromOutputs(mockEnv, mockCmd, mockSettings, failure, Set[Output](o0))
      val successfulExecs = Execution.fromOutputs(mockEnv, mockCmd, mockSettings, success, Set[Output](o1, o3))

      filter.record(Seq(failedExecs, successfulExecs))

      // Missing record:  'hasDifferentHash' --> false
      //                  'isOlder --> false
      //                  'needsToBeRun' --> true
      assert(cachedOutput3.isMissing)
      assert(!filter.hasDifferentHash(cachedOutput3))
      assert(!filter.isOlder(cachedOutput3))
      assert(filter.needsToBeRun(jobName, cachedOutput3))

      // Older record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'isOlder --> true
      //                                                'needsToBeRun' --> true
      val olderRec = cachedOutput1.withLastModified(Instant.ofEpochMilli(0))
      assert(!filter.hasDifferentHash(olderRec))
      assert(filter.isOlder(olderRec))
      assert(filter.needsToBeRun(jobName, olderRec))

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

      // Otherwise: 'needsToBeRun' --> false
      assert(!filter.needsToBeRun(jobName, cachedOutput1))
    }
  }

  test("command string comparison") {
    def cmdLineJob(cmd: String, outputs: Set[Output]): CommandLineJob = {
      CommandLineStringJob(
        commandLineString = cmd,
        workDir = PathUtils.getCurrentDirectory,
        executionEnvironment = mockEnv,
        outputs = outputs)
    }

    def mockJob(cmd: String, outputs: Set[Output]): MockJob = {
      new MockJob(
        toReturn = mockExecution,
        name = "mock job",
        inputs = Set.empty,
        outputs = outputs,
        delay = 0
      )
    }

    def execution(cmd: String, outputs: Set[Output]): Execution = {
      Execution.fromOutputs(mockEnv, cmd, mockSettings, CommandResult(0), outputs)
    }

    val cmd0 = "cmd0"
    val cmd1 = "cmd1"

    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      val o0o1 = Set[Output](o0, o1)

      filter.record(Iterable(execution(cmd0, o0o1)))

      assert(filter.findCommandLine(o0.location) === Some(cmd0))
      assert(filter.findCommandLine(o1.location) === Some(cmd0))

      val cmdLineJob0 = cmdLineJob(cmd0, o0o1)

      assert(!filter.hasNewCommandLine(cmdLineJob0))
      assert(filter.hasSameCommandLineIfAny(cmdLineJob0))

      filter.record(Iterable(execution(cmd1, Set[Output](o2))))
      val cmdLineJob1 = cmdLineJob("cmd1-altered", Set(o2))

      assert(filter.hasNewCommandLine(cmdLineJob1))
      assert(!filter.hasSameCommandLineIfAny(cmdLineJob1))

      // Non-CommandLineJob's shouldn't be affected by cmd string checks
      val nonCmdLineJob0 = mockJob(cmd0, o0o1)

      assert(!filter.hasNewCommandLine(nonCmdLineJob0))
      assert(filter.hasSameCommandLineIfAny(nonCmdLineJob0))

      val nonCmdLineJob1 = mockJob("cmd1-altered", Set(o2))

      assert(!filter.hasNewCommandLine(nonCmdLineJob1))
      assert(filter.hasSameCommandLineIfAny(nonCmdLineJob1))
    }
  }
}
