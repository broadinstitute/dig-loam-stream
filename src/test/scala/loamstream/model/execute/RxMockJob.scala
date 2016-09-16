package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.Output
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import loamstream.util.Observables
import loamstream.util.ValueBox


/**
 * @author kaan
 * @author clint
 * date: Sep 15, 2016
 */
final case class RxMockJob(
      override val name: String,  
      inputs: Set[LJob] = Set.empty,  
      outputs: Set[Output] = Set.empty, 
      runsAfter: Set[RxMockJob] = Set.empty, 
      fakeExecutionTimeInMs: Int = 0) extends LJob {

    private[this] val count = ValueBox(0)

    def executionCount = count.value

    private def waitIfNecessary(): Unit = {
      if(runsAfter.nonEmpty) {
        import ObservableEnrichments._
        val finalDepStates = Observables.sequence(runsAfter.toSeq.map(_.lastState))
        
        Futures.waitFor(finalDepStates.firstAsFuture)
      }
    }
    
    private def delayIfNecessary(): Unit = {
      if(fakeExecutionTimeInMs > 0) {
        Thread.sleep(fakeExecutionTimeInMs)
      }
    }
    
    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future(waitIfNecessary()).flatMap(_ => super.execute)
    }
    
    override protected def executeSelf(implicit context: ExecutionContext): Future[Result] = Future {
      
      trace(s"\t\tStarting job: $name")
      
      delayIfNecessary()
      
      trace(s"\t\t\tFinishing job: $name")
      
      count.mutate(_ + 1)
      
      LJob.SimpleSuccess(name)
    }

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def toString: String = name
  }