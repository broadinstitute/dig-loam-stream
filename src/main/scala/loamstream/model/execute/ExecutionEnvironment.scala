package loamstream.model.execute

/**
 * @author clint
 * Nov 22, 2016
 */
sealed trait ExecutionEnvironment {
  def isLocal: Boolean = this == ExecutionEnvironment.Local
  def isUger: Boolean = this == ExecutionEnvironment.Uger
  def isGoogle: Boolean = this == ExecutionEnvironment.Google
}

object ExecutionEnvironment {
  case object Local extends ExecutionEnvironment

  case object Uger extends ExecutionEnvironment

  case object Google extends ExecutionEnvironment

  def fromString(name: String): ExecutionEnvironment =
    if (name == Local.toString) { Local }
    else if (name == Uger.toString) { Uger }
    else if (name == Google.toString) { Google }
    else { throw new RuntimeException(s"$name is not one of the valid execution environments") }
}
