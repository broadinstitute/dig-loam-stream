package loamstream.compiler

import loamstream.loam.LoamSyntax
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScript

import scala.util.DynamicVariable

/**
 * @author clint
 * May 28, 2020
 * 
 * A class for compilable, evaluatable Loam code blocks.  Loam code in files to be run by LoamStream should be 
 * expressed as the body of an object that extends this class.  Note that this is also aliased as 
 * loamstream.LoamFile, so Loam code files can look like this:
 * 
 * object Foo extends loamstream.LoamFile {
 *   val x = store(...)
 *   
 *   cmd"echo 42 > $x".out(x)
 * }
 */
abstract class LoamFile extends LoamSyntax with LoamScript.LoamScriptBox {
  override def projectContext: LoamProjectContext = {
    val context = LoamFile.ContextHolder.projectContext
    
    require(
        context != null, //scalastyle:ignore null
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
    private[this] val contextVar: DynamicVariable[LoamProjectContext] = {
      new DynamicVariable(null) //scalastyle:ignore null
    }
    
    def projectContext: LoamProjectContext = contextVar.value
    
    def withContext[A](newProjectContext: LoamProjectContext)(thunk: => A): A = {
      contextVar.withValue(newProjectContext)(thunk)
    }
  }
}
