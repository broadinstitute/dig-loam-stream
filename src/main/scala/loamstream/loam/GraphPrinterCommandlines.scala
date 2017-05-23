package loamstream.loam

import loamstream.loam.LoamToken.{StoreRefToken, StoreToken, StringToken}
import loamstream.loam.ops.filters.LoamStoreFilterTool
import loamstream.loam.ops.mappers.LoamStoreMapperTool
import loamstream.model.Store

/** Prints file names and command lines in LoamGraph */
final case class GraphPrinterCommandlines(lineLength: Int) extends GraphPrinter {

  /** Prints a store */
  def print(store: Store.Untyped): String = store.pathOpt.map(_.toString).getOrElse("[file]")

  /** Prints a store ref */
  def print(storeRef: LoamStoreRef): String =
  storeRef.store.pathOpt.map(storeRef.pathModifier).map(_.toString).getOrElse("[file ref]")

  /** Prints a token */
  def print(token: LoamToken, graph: LoamGraph): String = token match {
    case StringToken(string) => string
    case StoreToken(store) => print(store)
    case StoreRefToken(storeRef) => print(storeRef)
  }

  /** Prints a tool */
  def print(tool: LoamTool): String = tool match {
    case cmdTool: LoamCmdTool => print(cmdTool)
    case nativeTool: LoamNativeTool[_] => print(nativeTool)
    case filterTool: LoamStoreFilterTool[_] => print(filterTool)
    case mapperTool: LoamStoreMapperTool[_, _] => print(mapperTool)
  }

  /** Prints a cmd tool */
  def print(tool: LoamCmdTool): String = tool.tokens.map(print(_, tool.graph)).mkString

  /** Prints a native tool */
  def print[T](tool: LoamNativeTool[T]): String = "[native tool]"

  /** Prints a filter tool */
  def print(tool: LoamStoreFilterTool[_]): String = "[filter tool]"

  /** Prints a mapper tool */
  def print(tool: LoamStoreMapperTool[_, _]): String = "[mapper tool]"

  /** Prints file names and command lines in LoamGraph */
  override def print(graph: LoamGraph): String =
  graph.ranks.map {
    case (tool, rank) => s"$rank: ${
      print(tool)
    }"
  }.mkString("\n")

}
