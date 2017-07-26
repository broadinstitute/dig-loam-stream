package loamstream.v2

import loamstream.model.LId

sealed trait Token {
  def render(implicit symbols: SymbolTable): String 
}

object Token {
  final case class StringToken(private val value: String) extends Token {
    override def render(implicit unused: SymbolTable): String = value
  }
  
  final case class StoreToken(storeId: LId) extends Token {
    override def render(implicit symbols: SymbolTable): String = symbols.resolveStore(storeId).render 
  }
}
