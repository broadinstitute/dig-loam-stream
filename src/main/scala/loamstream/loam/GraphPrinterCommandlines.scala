package loamstream.loam

import loamstream.loam.LoamToken.{MultiStoreToken, StoreToken, StringToken}
import loamstream.model.{Store, Tool}
import loamstream.loam.LoamToken.MultiToken

/** Prints file names and command lines in LoamGraph */
final case class GraphPrinterCommandlines(lineLength: Int) extends GraphPrinter {

  /** Prints a store */
  private def print(store: Store): String = store.pathOpt.map(_.toString).getOrElse("[file]")

  private def print(stores: Iterable[Store]): String = stores.map(print).mkString(" ")

  /** Prints a token */
  private def print(token: LoamToken, graph: LoamGraph): String = token match {
    case StringToken(string) => string
    case StoreToken(store) => print(store)
    case MultiStoreToken(stores) => print(stores)
    case MultiToken(tokens) => tokens.mkString(",")
  }

  /** Prints a tool */
  private def print(tool: Tool): String = tool match {
    case cmdTool: LoamCmdTool => printCmd(cmdTool)
    case nativeTool: NativeTool => printNative(nativeTool)
  }

  /** Prints a cmd tool */
  private def printCmd(tool: LoamCmdTool): String = tool.tokens.map(print(_, tool.graph)).mkString
  
  private def printNative(tool: NativeTool): String = s"${classOf[NativeTool].getSimpleName}(id=${tool.id})"

  /** Prints file names and command lines in LoamGraph */
  override def print(graph: LoamGraph): String = {
    graph.ranks.map {case (tool, rank) => s"$rank: ${print(tool)}" }.mkString("\n")
  }
}
