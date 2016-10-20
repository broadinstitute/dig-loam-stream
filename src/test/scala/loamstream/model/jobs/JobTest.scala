package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  
  //scalastyle:off magic.number
  
  import JobState._
  
  test("execute") {
    val job = MockJob(CommandResult(42))
    
    import Futures.waitFor
    import ObservableEnrichments._
    
    val states = job.states.until(_.isFinished).to[Seq].firstAsFuture
    
    job.execute(ExecutionContext.global)
    
    assert(waitFor(states) === Seq(NotStarted, Running, CommandResult(42)))
  }
  
  //scalastyle:on magic.number
}