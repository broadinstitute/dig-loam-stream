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

abstract class BufferingChunkRunner(
    environmentType: EnvironmentType,
    windowLength: Duration,
    maxJobBufferSize: Int,
    jobOracle: JobOracle, 
    shouldRestart: LJob => Boolean,
    scheduler: Scheduler = IOScheduler()
    ) extends ChunkRunnerFor(environmentType) {
  
  protected val submittedJobs: Subject[LJob] = PublishSubject()
  
  protected lazy val bufferedJobs: Observable[Seq[LJob]] = {
    submittedJobs.tumblingBuffer(windowLength, maxJobBufferSize, scheduler) 
  }
  
  protected lazy val executedJobs: Observable[Map[LJob, RunData]] = {
    bufferedJobs.map(_.toSet).filter(_.nonEmpty).flatMap(runChunk(jobOracle, shouldRestart))
  }
  
  protected def runChunk(
      jobOracle: JobOracle, 
      shouldRestart: LJob => Boolean)(chunk: Iterable[LJob]): Observable[Map[LJob, RunData]]
  
  override def run(jobs: Set[LJob]): Observable[Map[LJob, RunData]] = {
    
    jobs.foreach(submittedJobs.onNext)
    
    executedJobs
  }
}

