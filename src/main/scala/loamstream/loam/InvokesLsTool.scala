package loamstream.loam

import loamstream.model.Tool
import loamstream.model.Tool.DefaultStores
import loamstream.model.LId

/**
 * @author clint
 * Aug 11, 2020
 */
final case class InvokesLsTool(
    tagNameToRun: String)(implicit override val scriptContext: LoamScriptContext) extends Tool {
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = DefaultStores.empty
    
  override val id: LId = LId.newAnonId
}
