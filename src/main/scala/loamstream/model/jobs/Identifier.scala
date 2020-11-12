package loamstream.model.jobs

import loamstream.model.jobs.commandline.CommandLineJob

/**
 * @author clint
 * Nov 12, 2020
 */
object Identifier {
  def from(job: LJob): Option[String] = job match {
    case clj: CommandLineJob => Some(clj.commandLineString)
    case n: NativeJob => n.nameOpt
    case _ => None
  }
}
