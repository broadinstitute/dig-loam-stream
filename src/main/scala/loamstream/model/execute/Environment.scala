package loamstream.model.execute

import loamstream.conf.UgerConfig
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.uger.UgerDefaults
import loamstream.conf.UgerSettings

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class Environment(val tpe: EnvironmentType) {
  def isLocal: Boolean = tpe.isLocal

  def isGoogle: Boolean = tpe.isGoogle
  
  def isUger: Boolean = tpe.isUger
}

object Environment {
  final case object Local extends Environment(EnvironmentType.Local)

  final case class Uger(settings: UgerSettings) extends Environment(EnvironmentType.Uger)

  final case object Google extends Environment(EnvironmentType.Google)
}
