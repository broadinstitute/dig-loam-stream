package loamstream.model.execute

import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject
import loamstream.model.jobs.LJob
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.IOScheduler
import scala.concurrent.duration.Duration
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.RunData
import rx.lang.scala.subjects.ReplaySubject
import loamstream.util.Throwables
import rx.lang.scala.subjects.UnicastSubject

abstract class BufferingChunkRunner(
    environmentType: EnvironmentType,
    windowLength: Duration,
    maxJobBufferSize: Int,
    scheduler: Scheduler = IOScheduler()) extends ChunkRunnerFor(environmentType) {
  
  private val submittedJobs: Subject[LJob] = /*UnicastSubject()*//*PublishSubject()*/ReplaySubject()
  
  private lazy val executedJobs: Observable[Map[LJob, RunData]] = {
    import scala.concurrent.duration._
    
    val distinct = submittedJobs.distinct(_.id)//.subscribeOn(scheduler)
    
    val bufferedJobs = if(maxJobBufferSize == 0 || windowLength == 0.seconds) {
      distinct.map(Seq(_))
    } else {
      distinct.tumblingBuffer(windowLength, maxJobBufferSize, scheduler)
    }
    
    bufferedJobs.map(_.toSet).filter(_.nonEmpty).flatMap(runChunk).share
  }
  
  protected def runChunk(chunk: Iterable[LJob]): Observable[Map[LJob, RunData]]
  
  override def run(jobs: Set[LJob]): Observable[Map[LJob, RunData]] = {
    
    println(s"@@@@@@@@@ submitting (${jobs.size}) jobs: $jobs")
    
    jobs.foreach(submittedJobs.onNext)
    
    println(s"@@@@@@@@@ submitted (${jobs.size}) jobs: $jobs")
    
    executedJobs
  }
  
  override def stop(): Iterable[Throwable] = {
    println(s"@@@@@@@@@ Stopping ${this}")
    
    Throwables.failureOption(submittedJobs.onCompleted())
  }
}

