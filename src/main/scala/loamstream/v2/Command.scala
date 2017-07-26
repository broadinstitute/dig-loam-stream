package loamstream.v2

import loamstream.util.Functions
import loamstream.model.LId

sealed trait Command extends Tool {
  def commandLineParts: Seq[Token]
  
  def commandLine(implicit symbols: SymbolTable): String = commandLineParts.map(_.render).mkString
}

object Command {
  def apply(commandLineParts: Token*)(lazyInputs: => Set[LId])(implicit context: Context): Command = {
    DefaultCommand(commandLineParts: _*)(lazyInputs)(context)
  }
  
  private final case class DefaultCommand(
      commandLineParts: Token*)(lazyDeps: => Set[LId])(implicit override val context: Context) extends Command {
  
    private[this] val initReferencedStores: () => Set[LId] = Functions.memoize(() => lazyDeps)
    
    override def referencedStores: Set[LId] = initReferencedStores()
  }
  
}
