package loamstream.model.execute

import loamstream.conf.GoogleSettings
import loamstream.conf.LocalSettings
import loamstream.conf.Settings
import loamstream.conf.UgerSettings

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class Environment(val tpe: EnvironmentType) {
  def settings: Settings
  
  def isLocal: Boolean = tpe.isLocal

  def isGoogle: Boolean = tpe.isGoogle
  
  def isUger: Boolean = tpe.isUger
}

object Environment {
  final case object Local extends Environment(EnvironmentType.Local) {
    override def settings: Settings = LocalSettings
  }

  final case class Uger(ugerSettings: UgerSettings) extends Environment(EnvironmentType.Uger) {
    override def settings: Settings = ugerSettings
  }

  final case class Google(googleSettings: GoogleSettings) extends Environment(EnvironmentType.Google) {
    override def settings: Settings = googleSettings
  }
}
