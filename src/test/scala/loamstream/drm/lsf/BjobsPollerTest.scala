package loamstream.drm.lsf

import org.scalatest.FunSuite

/**
 * @author clint
 * May 15, 2018
 */
final class BjobsPollerTest extends FunSuite {
  test("parseBjobsOutputLine") {
    import BjobsPoller.parseBjobsOutputLine
    
    val actualOutputLine = "2842408 cgilbert DONE  research-rh7 ebi-cli-002 ebi5-270    helloworld[2] May 17 20:14"
    
    val result = parseBjobsOutputLine(actualOutputLine)
    
    assert(result === Some(LsfJobId("2842408", 2) -> LsfStatus.Done))
  }
  
  test("parseBjobsOutputLine - bad input") {
    fail("TODO")
  }
  
  test("parseBjobsOutput") { 
    import BjobsPoller.parseBjobsOutput
  
    val actualOutput = Seq( 
      "JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME",
      "2842408 cgilbert DONE  research-rh7 ebi-cli-002 ebi5-270    helloworld[2] May 17 20:14",
      "2842408 cgilbert PEND  research-rh7 ebi-cli-002 ebi6-116    helloworld[3] May 17 20:14",
      "2842408 cgilbert EXIT  research-rh7 ebi-cli-002 hx-noah-10-09 helloworld[1] May 17 20:14")
      
    val results = parseBjobsOutput(actualOutput)
    
    val expected = Seq(
        LsfJobId("2842408", 2) -> LsfStatus.Done,
        LsfJobId("2842408", 3) -> LsfStatus.Pending,
        LsfJobId("2842408", 1) -> LsfStatus.Exited)
        
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
    
    val expected = Seq("foo", "-w", "2842408[3,1]")
    
    assert(tokens === expected)
  }
}
