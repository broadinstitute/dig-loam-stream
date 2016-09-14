package loamstream.model.execute

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.concurrent.duration.Duration

import loamstream.model.jobs.{ LJob, Output }
import loamstream.model.jobs.LJob._
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.ObservableEnrichments
import loamstream.util.Observables
import loamstream.util.Shot
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.IOScheduler

/**
 * @author kaan
 *         date: Aug 17, 2016
 */
final case class RxExecuter(
    runner: ChunkRunner,  
    windowLength: Duration = 30.seconds)(implicit executionContext: ExecutionContext) extends LExecuter with Loggable {
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    import Maps.Implicits._
    import ObservableEnrichments._
    val runnables = executable.jobs.toSeq.map(_.runnables).reduceOption(_ merge _).getOrElse(Observable.empty)
    
    val ioScheduler = IOScheduler()
    
    val chunks = runnables.distinct.tumbling(windowLength, runner.maxNumJobs, ioScheduler)
    
    val chunkResults = for {
      chunk <- chunks
      jobs <- chunk.to[Set]
      if jobs.nonEmpty
      _ = info(s"%%%%% RUNNING CHUNK: ${jobs.map(_.name)}")
      resultMap <- runner.run(jobs)
    } yield {
      resultMap
    }
    
    
    val futureMergedResults = for {
      mergedResults <- chunkResults.to[Seq].map(Maps.mergeMaps).firstAsFuture
    } yield {
      mergedResults.strictMapValues(Shot(_))
    }
    
    Await.result(futureMergedResults, timeout)
  }
}

object RxExecuter {
  def default: RxExecuter = new RxExecuter(asyncLocalChunkRunner(8))(ExecutionContext.global)

  def asyncLocalChunkRunner(maxJobs: Int): ChunkRunner = new ChunkRunner {

    import ExecuterHelpers._

    override def maxNumJobs = maxJobs
    
    override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Observable[Map[LJob, Result]] = {
      def exec(job: LJob): Observable[Map[LJob, Result]] = Observable.from(executeSingle(job))

      val resultObservables: Seq[Observable[Map[LJob, Result]]] = jobs.toSeq.map(exec)
      
      Observables.sequence(resultObservables).map(Maps.mergeMaps)
    }
  }

  final case class RxMockJob(
      override val name: String, 
      inputs: Set[LJob] = Set.empty, 
      outputs: Set[Output] = Set.empty,
      delay: Int = 0) extends LJob {

    private[this] val count = ValueBox(0)

    def executionCount = count.value

    private def waitIfNecessary(): Unit = {
      if (delay > 0) {
        Thread.sleep(delay)
      }
    }
    
    override protected def executeSelf(implicit context: ExecutionContext): Future[Result] = Future {
      trace(s"\t\tStarting job: $name")
      
      waitIfNecessary()
      
      trace(s"\t\t\tFinishing job: $name")
      
      count.mutate(_ + 1)
      
      LJob.SimpleSuccess(name)
    }

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def toString: String = name
  }
}