package loamstream.model.execute

import loamstream.model.execute.RxExecuter.Tracker
import loamstream.model.jobs.{JobState, LJob, NoOpJob, Output}
import loamstream.model.jobs.JobState.{NotStarted, Running, Succeeded}
import loamstream.model.jobs.LJob._
import loamstream.util._
import rx.lang.scala.subjects.PublishSubject

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import rx.lang.scala.Observable
import rx.lang.scala.schedulers.IOScheduler

/**
 * @author kaan
 *         date: Aug 17, 2016
 */
final case class RxExecuter(
    runner: ChunkRunner, 
    windowLength: Duration = 30.seconds,
    tracker: Tracker = Tracker())(implicit executionContext: ExecutionContext) extends LExecuter with Loggable {
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    val runnables = executable.jobs.toSeq.map(_.runnables).reduceOption(_ ++ _).getOrElse(Observable.empty)
    
    val ioScheduler = IOScheduler()
    
    import scala.concurrent.duration._
    
    val chunks = runnables.distinct.tumbling(windowLength, runner.maxNumJobs, ioScheduler)
    
    val chunkResults = for {
      chunk <- chunks
      jobs <- chunk.to[Set]
      _ = info(s"%%%%% RUNNING CHUNK: ${jobs.map(_.name)}")
      _ = tracker.addJobs(jobs)
      resultMap <- runner.run(jobs)
    } yield {
      resultMap
    }
    
    import ObservableEnrichments._
    import Maps.Implicits._
    
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

    /*override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, Result]] = {
      //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
      //on the first failure, like the old code did.
      val jobResultFutures = jobs.iterator.map(executeSingle)

      //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure
      //input jobs are evaluated before jobs that depend on them.
      val futureJobResults = Future.sequence(jobResultFutures).map(consumeUntilFirstFailure)

      futureJobResults.map(Maps.mergeMaps)
    }*/
    
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

    override protected def executeSelf(implicit context: ExecutionContext): Future[Result] = Future {
      trace(s"\t\tStarting job: $name")
      
      if (delay > 0) {
        Thread.sleep(delay)
      }
      
      trace(s"\t\t\tFinishing job: $name")
      
      count.mutate(_ + 1)
      
      LJob.SimpleSuccess(name)
    }

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def toString: String = name
  }

  final case class Tracker() {
    private val executionSeq: ValueBox[Seq[Set[LJob]]] = ValueBox(Vector.empty)

    def addJobs(jobs: Set[LJob]): Unit = executionSeq.mutate(_ :+ jobs)

    def jobExecutionSeq = executionSeq.value
  }

}