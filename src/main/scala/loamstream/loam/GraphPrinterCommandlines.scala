package loamstream.loam

import loamstream.loam.LoamToken.{MultiStoreToken, StoreToken, StringToken}
import loamstream.model.{Store, Tool}
import loamstream.loam.LoamToken.MultiToken

/** Prints file names and command lines in LoamGraph */
final case class GraphPrinterCommandlines(lineLength: Int) extends GraphPrinter {

  /** Prints a store */
  def print(store: Store): String = store.pathOpt.map(_.toString).getOrElse("[file]")

  def print(stores: Iterable[Store]): String = stores.map(print).mkString(" ")

  /** Prints a token */
  def print(token: LoamToken, graph: LoamGraph): String = token match {
    case StringToken(string) => string
    case StoreToken(store) => print(store)
    case MultiStoreToken(stores) => print(stores)
    case MultiToken(tokens) => tokens.mkString(",")
  }

  /** Prints a tool */
  def print(tool: Tool): String = tool match {
    case cmdTool: LoamCmdTool => print(cmdTool)
  }

  /** Prints a cmd tool */
  def print(tool: LoamCmdTool): String = tool.tokens.map(print(_, tool.graph)).mkString

  /** Prints file names and command lines in LoamGraph */
  override def print(graph: LoamGraph): String = {
    graph.ranks.map {case (tool, rank) => s"$rank: ${print(tool)}" }.mkString("\n")
  }
}
