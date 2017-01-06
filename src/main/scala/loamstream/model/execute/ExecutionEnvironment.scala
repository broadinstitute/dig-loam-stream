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
}