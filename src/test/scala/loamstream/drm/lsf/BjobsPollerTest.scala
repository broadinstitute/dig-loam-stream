package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.drm.DrmStatus
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.time.Instant
import java.time.temporal.ChronoField
import java.time.LocalDateTime
import loamstream.model.quantities.CpuTime
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.quantities.Memory
import loamstream.drm.Queue
import loamstream.util.RunResults
import loamstream.util.Tries
import loamstream.drm.DrmTaskId

/**
 * @author clint
 * May 15, 2018
 */
final class BjobsPollerTest extends FunSuite {

  // scalastyle:off line.size.limit
  private val validStdOut = Seq(
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1]|EXIT |42        ",
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2]|DONE |    -     ",
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3]|DONE |    -     ")
  // scalastyle:on line.size.limit
  
  test("poll - happy path") {
    def pollFn(lsfJobIds: Set[DrmTaskId]): Try[RunResults] = {
      Success(RunResults("whatever", 0, validStdOut, Seq.empty))
    }
    
    val taskIds = Set(
        DrmTaskId("2842408", 1), 
        DrmTaskId("2842408", 2), 
        DrmTaskId("2842408", 3))
    
    val results = new BjobsPoller(pollFn).poll(taskIds)
    
    val expected: Map[DrmTaskId, Try[DrmStatus]] = Map(
      DrmTaskId("2842408", 1) -> Success(DrmStatus.CommandResult(42)),
      DrmTaskId("2842408", 2) -> Success(DrmStatus.CommandResult(0)),
      DrmTaskId("2842408", 3) -> Success(DrmStatus.CommandResult(0)))
    
    assert(results === expected)
  }
  
  test("poll - bjobs invocation failure") {
    def pollFn(lsfJobIds: Set[DrmTaskId]): Try[RunResults] = Tries.failure("blarg")
    
    val taskIds = Set(
        DrmTaskId("2842408", 1), 
        DrmTaskId("2842408", 2), 
        DrmTaskId("2842408", 3))
    
    val results = new BjobsPoller(pollFn).poll(taskIds)
    
    assert(results.keySet === taskIds)
    assert(results.values.forall(_.isFailure))
  }
  
  test("poll - something threw") {
    val msg = "blarg"
    
    def pollFn(lsfJobIds: Set[DrmTaskId]): Try[RunResults] = {
      Failure(new Exception(msg))
    }
    
    val taskIds = Set(
        DrmTaskId("2842408", 1), 
        DrmTaskId("2842408", 2), 
        DrmTaskId("2842408", 3))
    
    val results = new BjobsPoller(pollFn).poll(taskIds)
    
    assert(results.keySet === taskIds)
    assert(results.values.forall(_.failed.get.getMessage == msg))
  }
  
  test("runChunk - happy path") {
    def pollFn(lsfJobIds: Set[DrmTaskId]): Try[RunResults] = {
      Success(RunResults("whatever", 0, validStdOut, Seq.empty))
    }
    
    val taskIds = Set(DrmTaskId("2842408", 1), DrmTaskId("2842408", 2), DrmTaskId("2842408", 3))
    
    val results = new BjobsPoller(pollFn).runChunk(taskIds)
    
    val expected: Map[DrmTaskId, Try[DrmStatus]] = Map(
      DrmTaskId("2842408", 1) -> Success(DrmStatus.CommandResult(42)),
      DrmTaskId("2842408", 2) -> Success(DrmStatus.CommandResult(0)),
      DrmTaskId("2842408", 3) -> Success(DrmStatus.CommandResult(0))
    )
    
    assert(results === expected)
  }
  
  test("runChunk - bjobs invocation failure") {
    val msg = "blarg"
    
    def pollFn(lsfJobIds: Set[DrmTaskId]): Try[RunResults] = Failure(new Exception(msg))
    
    val taskIds = Set(DrmTaskId("2842408", 1), DrmTaskId("2842408", 2), DrmTaskId("2842408", 3))
    
    val results = new BjobsPoller(pollFn).runChunk(taskIds)
    
    assert(results.keySet === taskIds)
    assert(results.values.forall(_.failed.get.getMessage == msg))
  }
  
  test("parseBjobsOutputLine - exit code") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      // scalastyle:off line.size.limit
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1]|EXIT |42        "
      // scalastyle:on line.size.limit
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    val expected = Success(DrmTaskId("2842408", 1) -> DrmStatus.CommandResult(42))
    
    assert(result === expected)
  }
  
  test("parseBjobsOutputLine - no exit code, exited normally") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      // scalastyle:off line.size.limit
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2]|DONE |    -     "
      // scalastyle:on line.size.limit
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Success(DrmTaskId("2842408", 2) -> DrmStatus.CommandResult(0)))
  }
  
  test("parseBjobsOutputLine - no exit code, still running") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      // scalastyle:off line.size.limit
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2]|RUN  |    -     "
      // scalastyle:on line.size.limit
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Success(DrmTaskId("2842408", 2) -> DrmStatus.Running))
  }
  
  test("parseBjobsOutputLine - bad input") {
    import BjobsPoller.parseBjobsOutputLine
    
    assert(parseBjobsOutputLine("2842408                             blarg RUN       -     ").isFailure)
    assert(parseBjobsOutputLine("").isFailure)
    assert(parseBjobsOutputLine("   ").isFailure)
    assert(parseBjobsOutputLine("foo").isFailure)
    
    assert(
        parseBjobsOutputLine("2842408| LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2]| GLARG   |    -").isFailure)
    
    assert(
        parseBjobsOutputLine("2842408| LoamStream-826b3929-4810-4116-8502-5c60cd830d81| RUN    |   -").isFailure)     
  }
  
  test("parseBjobsOutput") { 
    import BjobsPoller.parseBjobsOutput
  
    val results = parseBjobsOutput(validStdOut)
    
    val expected = Seq(
        DrmTaskId("2842408", 1) -> DrmStatus.CommandResult(42),
        DrmTaskId("2842408", 2) -> DrmStatus.CommandResult(0),
        DrmTaskId("2842408", 3) -> DrmStatus.CommandResult(0))
        
    assert(results === expected)
  }
  
  test("parseBjobsOutput - bad input") {
    import BjobsPoller.parseBjobsOutput
      
    val actualOutput = Seq(
      // scalastyle:off line.size.limit
      "",
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1]|EXIT |42        ",
      " fooooo ",
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3]|DONE |    -     ",
      "2842408 LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] GLARG       -",
      "2842408|            LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2]|DONE |    -     ",
      "2842408 LoamStream-826b3929-4810-4116-8502-5c60cd830d81 RUN       -")
      // scalastyle:on line.size.limit

    val results = parseBjobsOutput(actualOutput)
    
    val expected = Seq(
        DrmTaskId("2842408", 1) -> DrmStatus.CommandResult(42),
        DrmTaskId("2842408", 3) -> DrmStatus.CommandResult(0),
        DrmTaskId("2842408", 2) -> DrmStatus.CommandResult(0))
        
    assert(results === expected)
  }
  
  test("parseBjobsOutput - one pending job") {
    import BjobsPoller.parseBjobsOutput
      
    val actualOutput = Seq(
      // scalastyle:off line.size.limit
      "3993061|                         LoamStream-e569ac4a-f6cc-4c66-b314-68cac844bbc1[1]|PEND |    -     ")
      // scalastyle:on line.size.limit

    val results = parseBjobsOutput(actualOutput)
    
    val expected = Seq(DrmTaskId("3993061", 1) -> DrmStatus.Queued)
        
    assert(results === expected)
  }
  
  test("makeTokens") {
    val executable = "foo"
    val lsfJobIds = Set(DrmTaskId("2842408", 3), DrmTaskId("2842408", 1))
    
    import BjobsPoller.makeTokens
    
    val tokens = makeTokens(executable, lsfJobIds)
    
    val expected = Seq(
        "foo", 
        "-noheader", 
        "-d", 
        "-r", 
        "-s",
        "-o",
        // scalastyle:off line.size.limit
        "jobid: job_name:-75 stat: exit_code: delimiter='|'",
        // scalastyle:on line.size.limit
        "2842408[3,1]")
    
    assert(tokens === expected)
  }
  
  test("parseCpuTime") {
    import BjobsPoller.parseCpuTime
    
    assert(parseCpuTime("").isFailure)
    assert(parseCpuTime(" ").isFailure)
    assert(parseCpuTime("123.456").isFailure)
    
    assert(parseCpuTime("1.23 seconds") === Success(CpuTime.inSeconds(1.23)))
    assert(parseCpuTime("1.23 second") === Success(CpuTime.inSeconds(1.23)))
    
    val zero = CpuTime.inSeconds(0)
    
    assert(parseCpuTime("0 seconds") === Success(zero))
    assert(parseCpuTime("0.0 seconds") === Success(zero))
    assert(parseCpuTime("0 second") === Success(zero))
    assert(parseCpuTime("0.0 second") === Success(zero))
  }
  
}
