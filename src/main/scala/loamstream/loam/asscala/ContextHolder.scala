package loamstream.loam.asscala

import loamstream.util.ValueBox
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext

object ContextHolder {
  private[this] val projectContextBox: ValueBox[LoamProjectContext] = ValueBox(null)
  
  def projectContext: LoamProjectContext = projectContextBox.value
  def projectContext_=(newContext: LoamProjectContext): Unit = projectContextBox.value = newContext
  
  def newScriptContext: LoamScriptContext = new LoamScriptContext(projectContext)
}
