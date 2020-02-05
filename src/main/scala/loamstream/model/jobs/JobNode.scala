package loamstream.model.jobs

import loamstream.util.ValueBox
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.Observable
import loamstream.util.Loggable
import loamstream.util.Observables
import rx.lang.scala.observables.ConnectableObservable
import rx.lang.scala.subjects.BehaviorSubject
import rx.lang.scala.subjects.SerializedSubject
import loamstream.model.jobs.log.JobLog
import rx.lang.scala.subjects.PublishSubject

/**
 * @author oliverr
 *         clint
 *         kyuksel
 * Date (as LJob): Dec 23, 2015
 * Factored out: Nov 14, 2017
 *
 * NB: Note liberal use of shareReplay (.share.cache) to (dramatically) reduce memory consumption.  Without
 * this, RxJava/RxScala makes one "copy" of any observable that multicasts *per* derived observable.
 * Since we resursively build Observable streams from trees of jobs, this led to pathological, exponential memory
 * use.
 */
trait JobNode extends Loggable {
  def job: LJob

  /** Any jobs this job depends on */
  def dependencies: Set[JobNode]

  /** Any jobs that depend on this job */
  def successors: Set[JobNode]
}

object JobNode {
  trait LazySucessors { self: JobNode =>
    protected def successorsFn: () => Set[JobNode]
    
    override lazy val successors: Set[JobNode] = successorsFn()
  }
}
