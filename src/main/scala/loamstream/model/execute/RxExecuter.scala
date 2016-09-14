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
    
    //An Observable stream of jobs; each job is emitted when it becomes runnable.
    //Note the use of 'distinct' to avoid running jobs more than once, if that job is depended on by multiple 'root' 
    //jobs in an LExecutable.  This is a bit brute-force, but allows for simpler logic in LJob.
    val runnables = executable.jobs.toSeq.map(_.runnables).reduceOption(_ merge _).getOrElse(Observable.empty).distinct
    
    val ioScheduler = IOScheduler()
    
    //An observable stream of "chunks" of runnable jobs, with each chunk represented as an observable stream.
    //Jobs are buffered up until the amount of time indicated by 'windowLength' elapses, or 'runner.maxNumJobs'
    //are collected.  When that happens, the buffered "chunk" of jobs is emitted.
    val chunks = runnables.tumbling(windowLength, runner.maxNumJobs, ioScheduler)
    
    val chunkResults = for {
      chunk <- chunks
      jobs <- chunk.to[Set]
      //jobs can be empty if no jobs become runnable during 'windowLength'
      if jobs.nonEmpty
      resultMap <- runner.run(jobs)
    } yield {
      resultMap
    }
    
    //Collect the results from each chunk, and merge them, producing a future holding the merged results
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
}