package loamstream.model.execute

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class Environment(val tpe: EnvironmentType) {
  def settings: Settings
  
  final def isLocal: Boolean = tpe.isLocal

  final def isGoogle: Boolean = tpe.isGoogle
  
  final def isUger: Boolean = tpe.isUger
  
  final def isLsf: Boolean = tpe.isLsf
}

object Environment {
  final case object Local extends Environment(EnvironmentType.Local) {
    override def settings: Settings = LocalSettings
  }

  final case class Uger(ugerSettings: DrmSettings) extends Environment(EnvironmentType.Uger) {
    override def settings: Settings = ugerSettings
  }
  
  final case class Lsf(lsfSettings: DrmSettings) extends Environment(EnvironmentType.Lsf) {
    override def settings: Settings = lsfSettings
  }

  final case class Google(googleSettings: GoogleSettings) extends Environment(EnvironmentType.Google) {
    override def settings: Settings = googleSettings
  }
  
  //TODO: Revisit
  def from(tpe: EnvironmentType, settings: Settings): Option[Environment] = settings match {
    case LocalSettings => Some(Local)
    case drmSettings: UgerDrmSettings if tpe.isUger => Some(Uger(drmSettings)) //what if tpe ISN'T Uger?
    case drmSettings: LsfDrmSettings if tpe.isLsf => Some(Lsf(drmSettings)) //what if tpe ISN'T LSF?
    case googleSettings: GoogleSettings => Some(Google(googleSettings))
    case _ => None
  }
  
  object Drm {
    def unapply(env: Environment): Option[DrmSettings] = env match {
      case Uger(settings) => Some(settings)
      case Lsf(settings) => Some(settings)
      case _ => None
    }
  }
}
