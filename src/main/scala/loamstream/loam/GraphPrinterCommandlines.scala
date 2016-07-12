package loamstream.loam

import loamstream.loam.LoamToken.{EnvToken, StoreToken, StringToken}

/** Prints file names and command lines in LoamGraph */
case class GraphPrinterCommandlines(lineLength: Int) extends GraphPrinter {

  /** Prints a store */
  def print(store: LoamStore): String = store.pathOpt.map(_.toString).getOrElse("[file]")

  /** Prints a token */
  def print(token: LoamToken, graph: LoamGraph): String = token match {
    case StringToken(string) => string
    case EnvToken(key) => "[key]"
    case StoreToken(store) => print(store)
  }

  /** Prints a tool */
  def print(tool: LoamTool): String = tool.tokens.map(print(_, tool.graph)).mkString

  /** Prints file names and command lines in LoamGraph */
  override def print(graph: LoamGraph): String =
    graph.ranks.map({ case (tool, rank) => s"$rank: ${print(tool)}" }).mkString("\n")

}
