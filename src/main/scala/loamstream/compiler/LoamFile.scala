package loamstream.compiler

import loamstream.loam.LoamSyntax
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScript

/**
 * @author clint
 * May 28, 2020
 */
abstract class LoamFile extends LoamSyntax with LoamScript.LoamScriptBox {
  override def projectContext: LoamProjectContext = {
    val context = LoamFile.ContextHolder.projectContext
    
    require(
        context != null,
        s"No ${LoamProjectContext.getClass.getSimpleName} set.  Set it with ContextHolder.projectContext = ...")
    
    context
  }

  override implicit lazy val scriptContext: LoamScriptContext = new LoamScriptContext(projectContext)
}

object LoamFile {
  /**
   * @author clint
   * May 28, 2020
   */
  private[compiler] object ContextHolder {
    private val contextVar: ThreadLocal[LoamProjectContext] = new ThreadLocal
    
    def projectContext: LoamProjectContext = contextVar.get
    
    def projectContext_=(newProjectContext: LoamProjectContext): Unit = contextVar.set(newProjectContext)
    
    def clear(): Unit = contextVar.remove()
  }
}
