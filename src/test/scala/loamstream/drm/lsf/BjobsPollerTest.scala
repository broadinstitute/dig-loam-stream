package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.drm.DrmStatus

/**
 * @author clint
 * May 15, 2018
 */
final class BjobsPollerTest extends FunSuite {
  test("parseBjobsOutputLine - exit code") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      // scalastyle:off line.size.limit
      "2842408                                                                                        helloworld[2] EXIT  42        ",
      // scalastyle:on line.size.limit   
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Some(LsfJobId("2842408", 2) -> DrmStatus.CommandResult(42, None)))
  }
  
  test("parseBjobsOutputLine - no exit code, exited normally") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      // scalastyle:off line.size.limit
      "2842408                                                                                        helloworld[2] DONE      -     ",
      // scalastyle:on line.size.limit   
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Some(LsfJobId("2842408", 2) -> DrmStatus.CommandResult(0, None)))
  }
  
  test("parseBjobsOutputLine - no exit code, still running") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = {
      // scalastyle:off line.size.limit
      "2842408                                                                                        helloworld[2] RUN       -     ",
      // scalastyle:on line.size.limit   
    }
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Some(LsfJobId("2842408", 2) -> DrmStatus.Running))
  }
  
  test("parseBjobsOutputLine - bad input") {
    fail("TODO")
  }
  
  test("parseBjobsOutput") { 
    import BjobsPoller.parseBjobsOutput
  
    // scalastyle:off line.size.limit
    val actualOutput = Seq( 
      "2842408                                                                                        helloworld[1] EXIT  42        ",
      "2842408                                                                                        helloworld[3] DONE      -     ",
      "2842408                                                                                        helloworld[2] DONE      -     ")
    // scalastyle:on line.size.limit      

    val results = parseBjobsOutput(actualOutput)
    
    val expected = Seq(
        LsfJobId("2842408", 1) -> DrmStatus.CommandResult(42, None),
        LsfJobId("2842408", 3) -> DrmStatus.CommandResult(0, None),
        LsfJobId("2842408", 2) -> DrmStatus.CommandResult(0, None))
        
    assert(results === expected)
  }
  
  test("parseBjobsOutput - bad input") {
    fail("TODO")
  }
  
  test("makeTokens") {
    //private[lsf] def makeTokens(actualExecutable: String, lsfJobIds: Set[LsfJobId]): Seq[String] = {
    
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
        """"jobid: job_name:-100 stat: exit_code:"""",
        "2842408[3,1]")
    
    assert(tokens === expected)
  }
}
