package loamstream.loam

import loamstream.model.Tool
import loamstream.model.LId
import loamstream.model.Tool.DefaultStores
import loamstream.util.Functions

/**
 * @author clint (this version)
 * @author oliverr (the prequel version)
 * Dec 19, 2019
 */
final class NativeTool private (
    val body: () => Any)
   (implicit override val scriptContext: LoamScriptContext) extends Tool {
  
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = DefaultStores.empty
    
  override val id: LId = LId.newAnonId
}

object NativeTool {
  def apply(f: => Any)(implicit scriptContext: LoamScriptContext): NativeTool = {
    new NativeTool(Functions.memoize(() => f))
  }
}
