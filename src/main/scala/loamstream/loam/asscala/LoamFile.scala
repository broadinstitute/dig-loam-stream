package loamstream.loam.asscala

import loamstream.loam.LoamSyntax
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScript

abstract class LoamFile extends LoamSyntax with LoamScript.LoamScriptBox {
  override /*protected */ def projectContext: LoamProjectContext = ContextHolder.projectContext

  override /*protected */ implicit lazy val scriptContext: LoamScriptContext = ContextHolder.newScriptContext
}
