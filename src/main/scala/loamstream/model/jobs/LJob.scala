package loamstream.model.jobs

import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.Environment
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
trait LJob extends JobNode {
  def executionEnvironment: Environment
  
  def workDirOpt: Option[Path] = None

  /** A descriptive name for this job */
  def name: String
  
  final val id: Int = LJob.nextId()

  /** Any inputs needed by this job */
  def inputs: Set[DataHandle]
  
  /** Any outputs produced by this job */
  def outputs: Set[DataHandle]
  
  //TODO
  override def job: LJob = this
}

object LJob {
  private[this] val idSequence: Sequence[Int] = Sequence()
  
  def nextId(): Int = idSequence.next() 
}
