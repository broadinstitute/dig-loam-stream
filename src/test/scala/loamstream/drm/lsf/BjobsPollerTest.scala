package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.drm.DrmStatus
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * May 15, 2018
 */
final class BjobsPollerTest extends FunSuite {
  test("poll - happy path") {
    def pollFn(lsfJobIds: Set[LsfJobId]): Try[RunResults] = {
      val stdout = Seq( 
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1] EXIT  42        ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3] DONE      -     ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ")
      
      Success(RunResults("whatever", 0, stdout, Seq.empty))
    }
    
    val lsfJobIds = Set(
        LsfJobId("2842408", 1).asString, 
        LsfJobId("2842408", 2).asString, 
        LsfJobId("2842408", 3).asString)
    
    val results = new BjobsPoller(pollFn).poll(lsfJobIds)
    
    val expected: Map[String, Try[DrmStatus]] = Map(
      LsfJobId("2842408", 1).asString -> Success(DrmStatus.CommandResult(42, None)),
      LsfJobId("2842408", 2).asString -> Success(DrmStatus.CommandResult(0, None)),
      LsfJobId("2842408", 3).asString -> Success(DrmStatus.CommandResult(0, None)),
    )
    
    assert(results === expected)
  }
  
  test("poll - bjobs invocation failure") {
    def pollFn(lsfJobIds: Set[LsfJobId]): Try[RunResults] = {
      val stdout = Seq( 
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1] EXIT  42        ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3] DONE      -     ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ")
      
      Success(RunResults("whatever", 42, stdout, Seq.empty))
    }
    
    val lsfJobIds = Set(
        LsfJobId("2842408", 1).asString, 
        LsfJobId("2842408", 2).asString, 
        LsfJobId("2842408", 3).asString)
    
    val results = new BjobsPoller(pollFn).poll(lsfJobIds)
    
    assert(results.keySet === lsfJobIds)
    assert(results.values.forall(_.isFailure))
  }
  
  test("poll - something threw") {
    val msg = "blarg"
    
    def pollFn(lsfJobIds: Set[LsfJobId]): Try[RunResults] = {
      Failure(new Exception(msg))
    }
    
    val lsfJobIds = Set(
        LsfJobId("2842408", 1).asString, 
        LsfJobId("2842408", 2).asString, 
        LsfJobId("2842408", 3).asString)
    
    val results = new BjobsPoller(pollFn).poll(lsfJobIds)
    
    assert(results.keySet === lsfJobIds)
    assert(results.values.forall(_.failed.get.getMessage == msg))
  }
  
  test("runChunk - happy path") {
    def pollFn(lsfJobIds: Set[LsfJobId]): Try[RunResults] = {
      val stdout = Seq( 
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1] EXIT  42        ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3] DONE      -     ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ")
      
      Success(RunResults("whatever", 0, stdout, Seq.empty))
    }
    
    val lsfJobIds = Set(LsfJobId("2842408", 1), LsfJobId("2842408", 2), LsfJobId("2842408", 3))
    
    val results = new BjobsPoller(pollFn).runChunk(lsfJobIds)
    
    val expected: Map[LsfJobId, Try[DrmStatus]] = Map(
      LsfJobId("2842408", 1) -> Success(DrmStatus.CommandResult(42, None)),
      LsfJobId("2842408", 2) -> Success(DrmStatus.CommandResult(0, None)),
      LsfJobId("2842408", 3) -> Success(DrmStatus.CommandResult(0, None)),
    )
    
    assert(results === expected)
  }
  
  test("runChunk - bjobs invocation failure") {
    def pollFn(lsfJobIds: Set[LsfJobId]): Try[RunResults] = {
      val stdout = Seq( 
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1] EXIT  42        ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3] DONE      -     ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ")
      
      Success(RunResults("whatever", 42, stdout, Seq.empty))
    }
    
    val lsfJobIds = Set(LsfJobId("2842408", 1), LsfJobId("2842408", 2), LsfJobId("2842408", 3))
    
    val results = new BjobsPoller(pollFn).runChunk(lsfJobIds)
    
    assert(results.keySet === lsfJobIds)
    assert(results.values.forall(_.isFailure))
  }
  
  test("runChunk - something threw") {
    val msg = "blarg"
    
    def pollFn(lsfJobIds: Set[LsfJobId]): Try[RunResults] = {
      Failure(new Exception(msg))
    }
    
    val lsfJobIds = Set(LsfJobId("2842408", 1), LsfJobId("2842408", 2), LsfJobId("2842408", 3))
    
    val results = new BjobsPoller(pollFn).runChunk(lsfJobIds)
    
    assert(results.keySet === lsfJobIds)
    assert(results.values.forall(_.failed.get.getMessage == msg))
  }
  
  test("parseBjobsOutputLine - exit code") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] EXIT  42        ",
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Some(LsfJobId("2842408", 2) -> DrmStatus.CommandResult(42, None)))
  }
  
  test("parseBjobsOutputLine - no exit code, exited normally") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      "2842408                                 LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ",
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Some(LsfJobId("2842408", 2) -> DrmStatus.CommandResult(0, None)))
  }
  
  test("parseBjobsOutputLine - no exit code, still running") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      "2842408                             LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] RUN       -     ",
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Some(LsfJobId("2842408", 2) -> DrmStatus.Running))
  }
  
  test("parseBjobsOutputLine - bad input") {
    import BjobsPoller.parseBjobsOutputLine
    
    assert(parseBjobsOutputLine("2842408                             blarg RUN       -     ") === None)
    assert(parseBjobsOutputLine("") === None)
    assert(parseBjobsOutputLine("   ") === None)
    assert(parseBjobsOutputLine("foo") === None)
    assert(parseBjobsOutputLine("2842408 LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] GLARG       -") === None)
    assert(parseBjobsOutputLine("2842408 LoamStream-826b3929-4810-4116-8502-5c60cd830d81 RUN       -") === None)     
  }
  
  test("parseBjobsOutput") { 
    import BjobsPoller.parseBjobsOutput
  
    val actualOutput = Seq( 
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1] EXIT  42        ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3] DONE      -     ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ")

    val results = parseBjobsOutput(actualOutput)
    
    val expected = Seq(
        LsfJobId("2842408", 1) -> DrmStatus.CommandResult(42, None),
        LsfJobId("2842408", 3) -> DrmStatus.CommandResult(0, None),
        LsfJobId("2842408", 2) -> DrmStatus.CommandResult(0, None))
        
    assert(results === expected)
  }
  
  test("parseBjobsOutput - bad input") {
    import BjobsPoller.parseBjobsOutput
  
    val actualOutput = Seq( 
      "",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1] EXIT  42        ",
      " fooooo ",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3] DONE      -     ",
      "2842408 LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] GLARG       -",
      "2842408                           LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ",
      "2842408 LoamStream-826b3929-4810-4116-8502-5c60cd830d81 RUN       -")

    val results = parseBjobsOutput(actualOutput)
    
    val expected = Seq(
        LsfJobId("2842408", 1) -> DrmStatus.CommandResult(42, None),
        LsfJobId("2842408", 3) -> DrmStatus.CommandResult(0, None),
        LsfJobId("2842408", 2) -> DrmStatus.CommandResult(0, None))
        
    assert(results === expected)
  }
  
  test("makeTokens") {
    val executable = "foo"
    val lsfJobIds = Set(LsfJobId("2842408", 3), LsfJobId("2842408", 1))
    
    import BjobsPoller.makeTokens
    
    val tokens = makeTokens(executable, lsfJobIds)
    
    val expected = Seq(
        "foo", 
        "-noheader", 
        "-d", 
        "-r", 
        "-s",
        "-o",
        "jobid: job_name:-100 stat: exit_code:",
        "2842408[3,1]")
    
    assert(tokens === expected)
  }
}
