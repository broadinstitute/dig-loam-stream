package loamstream.loam

import loamstream.model.Tool
import loamstream.model.Tool.DefaultStores
import loamstream.model.LId

/**
 * @author clint
 * Aug 11, 2020
 * 
 * A tool representing the invocation of LS as a job, managed by another LS instance.
 * This class exists to provide a graph node for this case, that tracks any setup commands needed by the child job,
 * (the preambles), such as `using` commands in the Uger context.
 */
final case class InvokesLsTool private (
    preambles: Seq[String] = Nil)
   (implicit override val scriptContext: LoamScriptContext) extends Tool with LId.IdBasedEquality {
  
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = DefaultStores.empty
    
  override val id: LId = LId.newAnonId
  
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
