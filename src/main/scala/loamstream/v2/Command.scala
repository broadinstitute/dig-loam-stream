package loamstream.v2

import loamstream.util.Functions
import loamstream.model.LId

final case class Command
    (commandLineParts: Token*)
    (lazyInputs: => Set[LId])
    (implicit override val context: Context) extends Tool {
  
  def commandLine(implicit symbols: SymbolTable): String = commandLineParts.map(_.render).mkString
  
  private[this] val initReferencedStores: () => Set[LId] = Functions.memoize(() => lazyInputs)
    
  override def referencedStores: Set[LId] = initReferencedStores()
}
