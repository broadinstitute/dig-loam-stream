package loamstream.model.execute

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
  
  //TODO: Revisit
  def from(tpe: EnvironmentType, settings: Settings): Option[Environment] = (tpe, settings) match {
    case (EnvironmentType.Local, LocalSettings) => Some(Local)
    case (EnvironmentType.Uger, ugerSettings: UgerSettings) => Some(Uger(ugerSettings))
    case (EnvironmentType.Google, googleSettings: GoogleSettings) => Some(Google(googleSettings))
    case _ => None
  }
}
