package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.Execution
import loamstream.model.jobs.OutputRecord
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.TestHelpers.dummyOutputStreams
import loamstream.TestHelpers.path
import loamstream.model.jobs.Output

/**
 * @author clint
 * Jul 2, 2018
 */
final class DbBackedExecutionRecorderTest extends FunSuite with ProvidesSlickLoamDao with ProvidesEnvAndResources {
  
  private val p0 = path("src/test/resources/for-hashing/foo.txt")
  private val p1 = path("src/test/resources/for-hashing/empty.txt")
  private val p2 = path("src/test/resources/for-hashing/subdir/bar.txt")
  private val nonexistentPath = path("non/existent/blah.txt")

  private val o0 = Output.PathOutput(p0)
  private val o1 = Output.PathOutput(p1)
  private val o2 = Output.PathOutput(p2)
  private val nonExistentOutput = Output.PathOutput(nonexistentPath)
  
  private val cachedOutput0 = o0.toOutputRecord
  private val cachedOutput1 = o1.toOutputRecord
  private val cachedOutput2 = o2.toOutputRecord
  private val cachedNonExistentOutput = nonExistentOutput.toOutputRecord

  private val failedOutput0 = failedOutput(p0)
  private val failedOutput1 = failedOutput(p1)
  private val failedOutput2 = failedOutput(p2)
  
  test("record() - no Executions") {
    createTablesAndThen {
      val recorder = new DbBackedExecutionRecorder(dao)

      assert(executions === Set.empty)

      recorder.record(Nil)

      assert(executions === Set.empty)
    }
  }

  test("record() - non-command-Execution") {
    def doTest(
        command: Option[String], 
        status: JobStatus, 
        result: Option[JobResult] = None): Unit = {
      createTablesAndThen {
        val recorder = new DbBackedExecutionRecorder(dao)
  
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
        
        recorder.record(Seq(e))
  
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
      val recorder = new DbBackedExecutionRecorder(dao)

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

      recorder.record(Seq(e))

      assertEqualFieldsFor(executions, Set(e))
    }
  }

  test("record() - failed command-Execution, no outputs") {
    createTablesAndThen {
      val recorder = new DbBackedExecutionRecorder(dao)

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
          outputs = Set.empty)

      recorder.record(Seq(e))

      assertEqualFieldsFor(executions, Set(e))
    }
  }

  test("record() - successful command-Execution, some outputs") {
    createTablesAndThen {
      val recorder = new DbBackedExecutionRecorder(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(0)

      assert(cr.isSuccess)

      val e = Execution.fromOutputs(mockEnv, mockCmd, cr, dummyOutputStreams, Set(o0, o1, o2))
      val withHashedOutputs = e.withOutputRecords(Set(cachedOutput0, cachedOutput1, cachedOutput2))

      recorder.record(Seq(e))

      assertEqualFieldsFor(executions, Set(withHashedOutputs))
    }
  }

  test("record() - failed command-Execution, some outputs") {
    createTablesAndThen {
      val recorder = new DbBackedExecutionRecorder(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(42)

      assert(cr.isFailure)

      val e = Execution.fromOutputs(mockEnv, mockCmd, cr, dummyOutputStreams, Set[Output](o0, o1, o2))

      recorder.record(Seq(e))

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
}