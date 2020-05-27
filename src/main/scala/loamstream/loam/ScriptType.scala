package loamstream.loam

/**
 * @author clint
 * May 27, 2020
 */
sealed abstract class ScriptType(val suffix: String) {
  def isLoam: Boolean = this == ScriptType.Loam
  
  def isScala: Boolean = this == ScriptType.Scala
}

object ScriptType {
  case object Loam extends ScriptType(".loam")
  case object Scala extends ScriptType(".scala")
}
