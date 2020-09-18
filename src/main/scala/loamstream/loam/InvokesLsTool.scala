package loamstream.loam

import loamstream.model.Tool
import loamstream.model.Tool.DefaultStores
import loamstream.model.LId

/**
 * @author clint
 * Aug 11, 2020
 */
final case class InvokesLsTool private (
    private val _tagNameToRun: () => String,
    preambles: Seq[String])(implicit override val scriptContext: LoamScriptContext) extends Tool {
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = DefaultStores.empty
    
  override val id: LId = LId.newAnonId
  
  def tagNameToRun: String = _tagNameToRun()
  
  def addPreamble(preamble: String): InvokesLsTool = copy(preambles = preambles :+ preamble)
}

object InvokesLsTool {
  def apply(_tagNameToRun: => String)(implicit scriptContext: LoamScriptContext): InvokesLsTool = {
    new InvokesLsTool(() => _tagNameToRun, preambles = Nil)
  }
  
  implicit object CanAddPreambleToInvokesLsTools extends CanAddPreamble[InvokesLsTool] {
    override def addPreamble(
        preamble: String, 
        orig: InvokesLsTool)(implicit scriptContext: LoamScriptContext): InvokesLsTool = {
      
      orig.addPreamble(preamble)
    }
  }
}
