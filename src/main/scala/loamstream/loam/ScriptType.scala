package loamstream.loam

/**
 * @author clint
 * May 27, 2020
 */
sealed abstract class ScriptType(val name: String) {
  def isLoam: Boolean = this == ScriptType.Loam
  
  def isScala: Boolean = this == ScriptType.Scala
  
  def suffix: String = s".${name}"
}

object ScriptType {
  case object Loam extends ScriptType("loam")
  case object Scala extends ScriptType("scala")
}
