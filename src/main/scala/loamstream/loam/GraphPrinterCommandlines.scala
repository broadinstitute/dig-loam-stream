package loamstream.loam

/** Prints file names and command lines in LoamGraph */
case class GraphPrinterCommandlines(lineLength: Int) extends GraphPrinter {
  /** Prints file names and command lines in LoamGraph */
  override def print(graph: LoamGraph): String =
    graph.ranks.map({ case (tool, rank) => s"$tool: $rank" }).mkString("\n")

}
