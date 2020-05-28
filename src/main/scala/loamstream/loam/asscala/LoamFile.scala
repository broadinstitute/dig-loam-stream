package loamstream.loam.asscala

import loamstream.loam.LoamSyntax
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScript
import loamstream.util.ValueBox

/**
 * @author clint
 * May 28, 2020
 */
abstract class LoamFile extends LoamSyntax with LoamScript.LoamScriptBox {
  override def projectContext: LoamProjectContext = {
    val context = ContextHolder.projectContext
    
    require(
        context != null,
        s"No ${LoamProjectContext.getClass.getSimpleName} set.  Set it with ContextHolder.projectContext = ...")
    
    context
  }

  override implicit lazy val scriptContext: LoamScriptContext = new LoamScriptContext(projectContext)
}
