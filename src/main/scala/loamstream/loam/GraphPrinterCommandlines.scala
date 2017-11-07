package loamstream.loam

import loamstream.loam.LoamToken.{MultiStoreToken, StoreRefToken, StoreToken, StringToken}
import loamstream.model.{Store, Tool}
import loamstream.loam.LoamToken.MultiToken

/** Prints file names and command lines in LoamGraph */
final case class GraphPrinterCommandlines(lineLength: Int) extends GraphPrinter {

  /** Prints a store */
  def print(store: Store): String = store.pathOpt.map(_.toString).getOrElse("[file]")

  /** Prints a store ref */
  def print(storeRef: LoamStoreRef): String =
  storeRef.store.pathOpt.map(storeRef.pathModifier).map(_.toString).getOrElse("[file ref]")

  def print(hasLocation: HasLocation): String = hasLocation match {
    case store: Store => print(store)
    case storeRef: LoamStoreRef => print(storeRef)
  }

  def print[H <: HasLocation](hasLocations: Iterable[H]): String = {
    hasLocations.map(print(_: HasLocation)).mkString(" ")
  }

  /** Prints a token */
  def print(token: LoamToken, graph: LoamGraph): String = token match {
    case StringToken(string) => string
    case StoreToken(store) => print(store)
    case StoreRefToken(storeRef) => print(storeRef)
    case MultiStoreToken(stores) => print(stores)
    case MultiToken(tokens) => tokens.mkString(",")
  }

  /** Prints a tool */
  def print(tool: Tool): String = tool match {
    case cmdTool: LoamCmdTool => print(cmdTool)
    case nativeTool: LoamNativeTool[_] => print(nativeTool)
  }

  /** Prints a cmd tool */
  def print(tool: LoamCmdTool): String = tool.tokens.map(print(_, tool.graph)).mkString

  /** Prints a native tool */
  def print[T](tool: LoamNativeTool[T]): String = "[native tool]"

  /** Prints file names and command lines in LoamGraph */
  override def print(graph: LoamGraph): String =
  graph.ranks.map {
    case (tool, rank) => s"$rank: ${
      print(tool)
    }"
  }.mkString("\n")

}
