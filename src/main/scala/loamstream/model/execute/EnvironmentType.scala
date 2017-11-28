package loamstream.model.execute

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class EnvironmentType(val name: String) {
  def isLocal: Boolean = this == EnvironmentType.Local

  def isGoogle: Boolean = this == EnvironmentType.Google
  
  def isUger: Boolean = this == EnvironmentType.Uger
}

object EnvironmentType {
  object Names {
    val Local = "local"
    val Google = "google"
    val Uger = "uger"
  }

  final case object Local extends EnvironmentType(Names.Local)

  final case object Uger extends EnvironmentType(Names.Uger)

  final case object Google extends EnvironmentType(Names.Google)
  
  def fromString(s: String): Option[EnvironmentType] = s.trim.toLowerCase match {
    case Names.Local => Some(Local)
    case Names.Google => Some(Google)
    case Names.Uger => Some(Uger)
    case _ => None
  }
}
