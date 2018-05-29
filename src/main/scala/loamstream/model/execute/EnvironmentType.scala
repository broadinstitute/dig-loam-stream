package loamstream.model.execute

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class EnvironmentType(val name: String) {
  final def isLocal: Boolean = this.isInstanceOf[EnvironmentType.Local.type]

  final def isGoogle: Boolean = this.isInstanceOf[EnvironmentType.Google.type]
  
  final def isUger: Boolean = this.isInstanceOf[EnvironmentType.Uger.type]
  
  final def isLsf: Boolean = this.isInstanceOf[EnvironmentType.Lsf.type]
}

object EnvironmentType {
  object Names {
    val Local = "local"
    val Google = "google"
    val Uger = "uger"
    val Lsf = "lsf"
  }

  final case object Local extends EnvironmentType(Names.Local)

  final case object Uger extends EnvironmentType(Names.Uger)
  
  final case object Lsf extends EnvironmentType(Names.Lsf)

  final case object Google extends EnvironmentType(Names.Google)
  
  def fromString(s: String): Option[EnvironmentType] = s.trim.toLowerCase match {
    case Names.Local => Some(Local)
    case Names.Google => Some(Google)
    case Names.Uger => Some(Uger)
    case Names.Lsf => Some(Lsf)
    case _ => None
  }
}
