package loamstream.model.execute

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class ExecutionEnvironment(val name: String) {
  def isLocal: Boolean = this == ExecutionEnvironment.Local

  // TODO: Can we do without isInstanceOf?
  def isUger: Boolean = this.isInstanceOf[ExecutionEnvironment.Uger]

  def isGoogle: Boolean = this == ExecutionEnvironment.Google
}

object ExecutionEnvironment {
  import scala.concurrent.duration._

  case object Local extends ExecutionEnvironment("Local")

  final case class Uger(mem: Memory = Memory.inGb(1),
                        cores: Int = 1,
                        maxRunTime: Duration = 2.hours)
    extends ExecutionEnvironment(Uger.name)

  // TODO Shouldn't need this companion object but couldn't extend ExecutionEnvironment without it for some reason
  object Uger {
    val name: String = "Uger"
  }

  case object Google extends ExecutionEnvironment("Google")

  def fromString(s: String): ExecutionEnvironment = s match {
    case Local.name => Local
    // TODO: Does this method make sense for Uger still?
    case Uger.name => Uger()
    case Google.name => Google
    case _ => throw new RuntimeException(
      s"$s must be one of the valid execution environments: ${Local.name}, ${Uger.name} or ${Google.name}")
  }
}
