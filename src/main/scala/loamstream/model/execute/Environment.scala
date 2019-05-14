package loamstream.model.execute

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed trait Environment {
  
}

object Environment {
  final case object Local extends Environment {
    
  }

  final case class Uger(ugerSettings: DrmSettings) extends Environment {
    
  }
  
  final case class Lsf(lsfSettings: DrmSettings) extends Environment {
    
  }

  final case class Google(googleSettings: GoogleSettings) extends Environment {
    
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
