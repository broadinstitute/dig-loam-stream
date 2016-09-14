package loamstream.loam

import loamstream.loam.LoamScript.scriptsPackage
import loamstream.util.StringUtils

/** A named Loam script */
object LoamScript {
  val scriptsPackage = "loamstream.loam.scripts"

  var scriptNameCounter: Int = 0
  val scriptNameCounterLock = new AnyRef

  val generatedNameBase = "LoamScript"

  def withGeneratedName(code: String): LoamScript = scriptNameCounterLock.synchronized({
    val name = s"$generatedNameBase${StringUtils.leftPadTo(scriptNameCounter.toString, "0", 3)}"
    scriptNameCounter += 1
    LoamScript(name, code)
  })
}

/** A named Loam script */
case class LoamScript(name: String, code: String) {

  def longName: String = s"$scriptsPackage.$name"

}
