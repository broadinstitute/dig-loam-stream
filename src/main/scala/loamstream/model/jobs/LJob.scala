package loamstream.model.jobs

import java.nio.file.Path

import loamstream.model.execute.Settings
import loamstream.util.Sequence

/**
 * @author oliverr
 *         clint
 *         kyuksel
 * date: Dec 23, 2015
 */
trait LJob extends JobNode {
  def initialSettings: Settings
  
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
