package loamstream.model.execute

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class ExecutionEnvironment(val name: String) {
  def isLocal: Boolean = this == ExecutionEnvironment.Local

  def isUger: Boolean = this == ExecutionEnvironment.Uger

  def isGoogle: Boolean = this == ExecutionEnvironment.Google
}

object ExecutionEnvironment {

  case object Local extends ExecutionEnvironment("Local")

  case object Uger extends ExecutionEnvironment("Uger")

  case object Google extends ExecutionEnvironment("Google")

  def fromString(s: String): ExecutionEnvironment = s match {
    case Local.name => Local
    case Uger.name => Uger
    case Google.name => Google
    case _ => throw new RuntimeException(
      s"$s must be one of the valid execution environments: ${Local.name}, ${Uger.name} or ${Google.name}")
  }
}
