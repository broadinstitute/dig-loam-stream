package loamstream.loam

/** Prints LoamGraph in human readable form for educational and debugging purposes */
trait GraphPrinter {
  /** Prints LoamGraph in human readable form for educational and debugging purposes */
  def print(graph: LoamGraph): String
}

object GraphPrinter {
  def byId(idLength: Int): GraphPrinterById = GraphPrinterById(idLength)

  def byLine(lineLength: Int): GraphPrinterCommandlines = GraphPrinterCommandlines(lineLength)
}
