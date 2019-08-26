package loamstream.v2

import loamstream.model.LId
import loamstream.util.ValueBox

final case class SymbolTable(stores: Map[LId, Store], tools: Map[LId, Tool]) {
  def +(store: Store): SymbolTable = copy(stores = stores + (store.id -> store))
  
  def +(tool: Tool): SymbolTable = copy(tools = tools + (tool.id -> tool))
  
  //TODO: Throw like this?
  def resolveStore(storeId: LId): Store = stores(storeId)
  
  //TODO: Throw like this?
  def resolveTool(toolId: LId): Tool = tools(toolId)
}

object SymbolTable {
  val Empty: SymbolTable = SymbolTable(stores = Map.empty, tools = Map.empty)
}
