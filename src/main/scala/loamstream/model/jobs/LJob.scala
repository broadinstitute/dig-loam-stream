package loamstream.model.jobs

import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.ExecutionEnvironment
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject
import loamstream.util.Sequence

/**
 * @author oliverr
 *         clint
 *         kyuksel
 * date: Dec 23, 2015
 */
trait LJob extends Loggable {
  def executionEnvironment: ExecutionEnvironment
  
  def workDirOpt: Option[Path] = None

  private[this] val snapshotRef: ValueBox[JobSnapshot] = ValueBox(JobSnapshot(JobStatus.NotStarted, 0))

  /** This job's 'state' (status and run count) at this instant. */ 
  private def snapshot: JobSnapshot = snapshotRef()
  
  //NB: Needs to be a ReplaySubject for correct operation
  private[this] val runsEmitter: Subject[JobRun] = ReplaySubject[JobRun]()
  
  /**
   * An Observable that emits JobRuns of this job.  These are (effectively) tuples of (job, status, runCount), 
   * which allows using .distinct this and derived Observables, solving the problem where multiple jobs with 
   * the same upstream dependency would cause the dependency to be run more than necessary under certain 
   * conditions.
   */
  protected final def runs: Observable[JobRun] = runsEmitter
  
  /** This job's current status */
  final def status: JobStatus = snapshotRef().status
  
  /** The number of times this job has transitioned to `Running` status */
  def runCount: Int = snapshotRef().runCount
  
  def print(
      indent: Int = 0, 
      doPrint: LJob => String => Unit = _ => debug(_), 
      header: Option[String] = None): Unit = {
    
    var visited: Set[LJob] = Set.empty 
    
    def loop(
        job: LJob,
        indent: Int = 0, 
        doPrint: LJob => String => Unit = _ => debug(_), 
        header: Option[String] = None): Unit = {
      
      val indentString = s"${"-" * indent} >"
  
      header.foreach(doPrint(job))
      
      doPrint(job)(s"$indentString (${job.status})${job}")
      
      visited += job
      
      job.inputs.filterNot(visited.contains).foreach(loop(_, indent + 2, doPrint))
    }
    
    loop(this, indent, doPrint, header)
  }

  /** A descriptive name for this job */
  def name: String
  
  val id: String = LJob.nextId()

  /** Any jobs this job depends on */
  def inputs: Set[LJob]

  /** Any outputs produced by this job */
  def outputs: Set[Output]

  /**
   * An Observable producing a all the runnable jobs among this job, its dependencies, their dependencies,
   * and so on, as soon as those jobs become runnable.  A job becomes runnable when all its dependencies are finished
   * - either successfully or with JobStatus.FailedPermanently - or if it fails (to facilitate restarting).  
   * If a job has no dependencies, it's runnable immediately. (See selfRunnables)
   */
  final lazy val runnables: Observable[JobRun] = {

    //Multiplex the streams of runnable jobs starting from each of our dependencies
    val dependencyRunnables = {
      if(inputs.isEmpty) { Observable.empty }
      //NB: Note the use of merge instead of ++; this ensures that we don't emit jobs from the sub-graph rooted at
      //one dependency before the other dependencies, but rather emit all the streams of runnable jobs "together".
      else { Observables.merge(inputs.toSeq.map(_.runnables)) }
    }

    //Emit the current job *after* all our dependencies
    (dependencyRunnables ++ selfRunnables)
  }
  
  /**
   * An observable that will emit this job when all this job's dependencies are finished, and then once for every
   * time this job fails with a non-Terminal status.
   * If the this job has no dependencies, this job is emitted immediately.  
   */
  private[jobs] lazy val selfRunnables: Observable[JobRun] = {
    def isNonTerminalFailure(jobRun: JobRun): Boolean = jobRun.status.isFailure && !jobRun.status.isTerminal
    
    //NB: We can just filter here, instead of using something like 
    //runs.filter(isFailure).takeUntil(isTerminal)
    //Since transitioning to a terminal status will shut down runsEmitter (aliased as "run") and observables
    //derived from it.
    lazy val nonTerminalFailures = runs.filter(isNonTerminalFailure)
    
    //NB: We run once, and then once per failure, until we transition to a terminal status. 
    //ChunkRunners/Executers are responsible for setting our status to FailedPermanently if they will no 
    //longer run us.
    def selfJobRun = jobRunFrom(snapshot)

    def justUs: Observable[JobRun] = {
      trace(s"selfRunnables '$id': justUs() ('$name')")
      
      Observable.just(selfJobRun) ++ nonTerminalFailures
    }

    //Return an observable that will ONLY produce a JobRun for this job with the status 'FailedPermanently'.
    //NB: As a side effect, transition this job to the state 'FailedPermanently'. :/
    def stopDueToDependencyFailure(): Observable[JobRun] = {
      trace(s"selfRunnables '$id': stopDueToDependencyFailure() ('$name')")
      
      transitionTo(JobStatus.CouldNotStart)
      
      val couldNotStartSnapshot = snapshot.withStatus(JobStatus.CouldNotStart)
      
      Observable.just(jobRunFrom(couldNotStartSnapshot))
    }

    if(inputs.isEmpty) {
      trace(s"selfRunnables '$id': no deps, just us ('$name')")
      
      justUs
    } else {
      for {
        inputStatuses <- finalInputStatuses
        _ = debug(s"selfRunnables '$id': deps finished with statuses: $inputStatuses ('$name')")
        anyInputFailures = inputStatuses.exists(_.isFailure)
        runnable <- if(anyInputFailures) stopDueToDependencyFailure() else justUs
      } yield {
        runnable
      }
    }
  }

  /**
   * An observable stream of statuses emitted by this job, each one reflecting a status this job transitioned to.
   */
  lazy val statuses: Observable[JobStatus] = runsEmitter.map(_.status)

  /**
   * The "terminal" status emitted by this job: the one that indicates the job is finished for any reason.
   * Will fire at most one time.
   */
  protected[jobs] lazy val lastStatus: Observable[JobStatus] = statuses.filter(_.isTerminal).first

  /**
   * An observable that will emit a sequence containing all our dependencies' "terminal" statuses.
   * When this fires, our dependencies are finished.
   */
  protected[jobs] lazy val finalInputStatuses: Observable[Seq[JobStatus]] = {
    Observables.sequence(inputs.toSeq.map(_.lastStatus))
  }
  
  /**
   * Sets the status of this job to be newStatus, and emits a JobRun with the new status to any observers.
   * Also bumps this job's run count if the current status *is not* `Running` and the new status *is* `Running`.
   * If `newStatus` is a terminal status, all Observables derived from this job will be shut down.   
   *
   * @param newStatus the new status to set for this job
   */
  final def transitionTo(newStatus: JobStatus): Unit = {
    debug(s"Status change to $newStatus (run count ${runCount}) for job: ${this}")
    
    val newSnapshot = snapshotRef.mutateAndGet(_.transitionTo(newStatus))
    
    if(newSnapshot.status.isRunning) {
      info(s"Now running: ${this}")
    }
    
    runsEmitter.onNext(jobRunFrom(newSnapshot))
    
    //Shut down all Observables derived from this job when we will no longer emit any events.
    if(newStatus.isTerminal) {
      trace(s"$newStatus is terminal; emitting no more JobRuns from job: ${this}")
      
      runsEmitter.onCompleted()
    }
  }
  
  private def jobRunFrom(snapshot: JobSnapshot): JobRun = JobRun(this, snapshot.status, snapshot.runCount)

  /**
   * Implementions of this method will do any actual work to be performed by this job
   */
  def execute(implicit context: ExecutionContext): Future[Execution]

  protected def doWithInputs(newInputs: Set[LJob]): LJob

  final def withInputs(newInputs: Set[LJob]): LJob = {
    if (inputs eq newInputs) { this }
    else { doWithInputs(newInputs) }
  }
}

object LJob {
  private[this] val idSequence: Sequence[Int] = Sequence()
  
  def nextId(): String = idSequence.next().toString 
}
