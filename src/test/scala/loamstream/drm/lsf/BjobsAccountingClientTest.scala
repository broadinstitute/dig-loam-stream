package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.drm.Queue
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * May 21, 2018
 */
final class BjobsAccountingClientTest extends FunSuite {
  
  private val actualBjobsOutput = "ebi6-066              research-rh7"
  
  test("makeTokens") {
    val jobId = "fooasdf"
    val executable = "aslkdjalskjd"
    
    import BjobsAccountingClient.makeTokens
    
    val expected = Seq(
        executable, 
        "-noheader", //Don't print a header row
        "-d",        //"Displays information about jobs that finished recently"
        "-r",        //Displays running jobs
        "-s",        //Display suspended jobs
        "-o",        //Specify output columns
        "exec_host:-100 queue:-100", 
        jobId)
        
    assert(makeTokens(executable, jobId) === expected)
  }
  
  test("parseBjobsOutput") {
    import BjobsAccountingClient.parseBjobsOutput
    
    assert(parseBjobsOutput(Nil) === None)
    assert(parseBjobsOutput(Seq("asdsadasdasd", actualBjobsOutput)) === None)
    
    val expected = BjobsAccountingClient.AccountingInfo("ebi6-066", Queue("research-rh7"))
    
    assert(parseBjobsOutput(Seq(actualBjobsOutput)) === Some(expected))
    assert(parseBjobsOutput(Seq(actualBjobsOutput, "asdasdasd", "asdasd")) === Some(expected))
  }
    
  test("getExecutionNode - happy path") {
    val client = new BjobsAccountingClient( _ => 
      Success(RunResults("foo", 0, Seq(actualBjobsOutput), Nil))
    )
    
    assert(client.getExecutionNode("asdf") === Some("ebi6-066"))
  }
  
  test("getExecutionNode - bad output from bjobs") {
    val client = new BjobsAccountingClient( _ => 
      Success(RunResults("foo", 0, Seq("asjdghjasdghjhasdg"), Nil))
    )
    
    assert(client.getExecutionNode("asdf") === None)
  }
  
  test("getExecutionNode - something threw") {
    val client = new BjobsAccountingClient( _ => 
      Failure(new Exception("blerg"))
    )
    
    assert(client.getExecutionNode("asdf") === None)
  }
  
  test("getQueue - happy path") {
    val client = new BjobsAccountingClient( _ => 
      Success(RunResults("foo", 0, Seq(actualBjobsOutput), Nil))
    )
    
    assert(client.getQueue("asdf") === Some(Queue("research-rh7")))
  }
  
  test("getQueue - bad output from bjobs") {
    val client = new BjobsAccountingClient( _ => 
      Success(RunResults("foo", 0, Seq("asjdghjasdghjhasdg"), Nil))
    )
    
    assert(client.getQueue("asdf") === None)
  }
  
  test("getQueue - something threw") {
    val client = new BjobsAccountingClient( _ => 
      Failure(new Exception("blerg"))
    )
    
    assert(client.getQueue("asdf") === None)
  }
}
