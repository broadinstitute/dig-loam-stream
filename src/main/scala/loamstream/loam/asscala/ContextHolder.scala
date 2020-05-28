package loamstream.loam.asscala

import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext

/**
 * @author clint
 * May 28, 2020
 */
object ContextHolder {
  private val contextVar: ThreadLocal[LoamProjectContext] = new ThreadLocal
  
  def projectContext: LoamProjectContext = contextVar.get
  
  def projectContext_=(newProjectContext: LoamProjectContext): Unit = contextVar.set(newProjectContext)
  
  def clear(): Unit = contextVar.remove()
}
