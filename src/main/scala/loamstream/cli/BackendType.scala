package loamstream.cli

/**
 * @author clint
 * Nov 10, 2016
 */
sealed trait BackendType {
  def name: String = toString
  
  final def isLocal: Boolean = this == BackendType.Local
  
  final def isUger: Boolean = this == BackendType.Uger
}

object BackendType {
  case object Local extends BackendType
  case object Uger extends BackendType
  
  def values: Set[BackendType] = Set(Local, Uger) 
  
  def byName(name: String): Option[BackendType] = values.find(_.name.toLowerCase == name.trim.toLowerCase)
}