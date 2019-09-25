package loamstream.model.execute

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class EnvironmentType private (val name: String) {
  final def isLocal: Boolean = this == EnvironmentType.Local

  final def isGoogle: Boolean = this == EnvironmentType.Google
  
  final def isUger: Boolean = this == EnvironmentType.Uger
  
  final def isLsf: Boolean = this == EnvironmentType.Lsf
  
  final def isAws: Boolean = this == EnvironmentType.Aws
}

object EnvironmentType {
  object Names {
    val Local = "local"
    val Google = "google"
    val Uger = "uger"
    val Lsf = "lsf"
    val Aws = "aws"
  }

  final case object Local extends EnvironmentType(Names.Local)

  final case object Uger extends EnvironmentType(Names.Uger)
  
  final case object Lsf extends EnvironmentType(Names.Lsf)

  final case object Google extends EnvironmentType(Names.Google)
  
  final case object Aws extends EnvironmentType(Names.Aws)
  
  def fromString(s: String): Option[EnvironmentType] = s.trim.toLowerCase match {
    case Names.Local => Some(Local)
    case Names.Google => Some(Google)
    case Names.Uger => Some(Uger)
    case Names.Lsf => Some(Lsf)
    case Names.Aws => Some(Aws)
    case _ => None
  }
}
