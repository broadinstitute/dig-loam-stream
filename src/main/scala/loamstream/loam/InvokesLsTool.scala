package loamstream.loam

import loamstream.model.Tool
import loamstream.model.Tool.DefaultStores
import loamstream.model.LId

/**
 * @author clint
 * Aug 11, 2020
 */
final case class InvokesLsTool private (
    preambles: Seq[String] = Nil)(implicit override val scriptContext: LoamScriptContext) extends Tool {
  
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = DefaultStores.empty
    
  override val id: LId = LId.newAnonId
  
  override def equals(other: Any): Boolean = other match {
    case that: InvokesLsTool => this.id == that.id
    case _ => false
  }
  
  override def hashCode: Int = id.hashCode
  
  def addPreamble(preamble: String): InvokesLsTool = copy(preambles = preambles :+ preamble)
}

object InvokesLsTool {
  implicit object CanAddPreambleToInvokesLsTools extends CanAddPreamble[InvokesLsTool] {
    override def addPreamble(
        preamble: String, 
        orig: InvokesLsTool)(implicit scriptContext: LoamScriptContext): InvokesLsTool = {
      
      orig.addPreamble(preamble)
    }
  }
}
