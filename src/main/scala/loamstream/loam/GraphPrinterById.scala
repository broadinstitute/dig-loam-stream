package loamstream.loam

import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.LoamGraph.StoreEdge.{PathEdge, ToolEdge}
import loamstream.loam.LoamToken.{StoreRefToken, StoreToken, StringToken}
import loamstream.model.LId
import loamstream.util.SourceUtils

import scala.reflect.runtime.universe.Type

/** Prints LoamGraph for educational and debugging purposes exposing ids */
final case class GraphPrinterById(idLength: Int) extends GraphPrinter {

  /** Prints id */
  def print(id: LId): String = id.name

  /** Prints id */
  def print(tpe: Type, fully: Boolean): String =
  if (fully) SourceUtils.fullTypeName(tpe) else SourceUtils.shortTypeName(tpe)

  /** Prints store */
  def print(store: LoamStore, fully: Boolean): String = s"@${print(store.id)}[${print(store.sig.tpe, fully)}]"

  /** Prints cmd tool */
  def print(tool: LoamTool): String = tool match {
    case cmdTool: LoamCmdTool => print(cmdTool)
    case nativeTool: LoamNativeTool => print(nativeTool)
  }

  /** Prints cmd tool */
  def print(tool: LoamCmdTool): String = print(tool, tool.graphBox.value)

  /** Prints cmd tool */
  def print(tool: LoamNativeTool): String = "[native tool]"

  /** Prints prefix symbol to distinguish input and output stores */
  def printIoPrefix(tool: LoamCmdTool, store: LoamStore, graph: LoamGraph): String =
  graph.storeSources.get(store) match {
    case Some(StoreEdge.ToolEdge(sourceTool)) if sourceTool == tool => ">"
    case _ => "<"
  }

  /** Prints tool */
  def print(tool: LoamCmdTool, graph: LoamGraph): String = {
    def toString(token: LoamToken): String = token match {
      case StringToken(string) => string
      case StoreToken(store) =>
        val ioPrefix = printIoPrefix(tool, store, graph)
        s"$ioPrefix${print(store, fully = false)}"
      case StoreRefToken(storeRef) =>
        val ioPrefix = printIoPrefix(tool, storeRef.store, graph)
        s"$ioPrefix${print(storeRef.store, fully = false)}"
    }

    val tokenString = tool.tokens.map(toString).mkString

    s"#${print(tool.id)}[$tokenString]"
  }

  /** Prints store edge */
  def print(source: StoreEdge): String = source match {
    case PathEdge(path) => path.toString
    case ToolEdge(tool) => s"#${print(tool.id)}"
  }

  /** Prints LoamGraph for educational and debugging purposes exposing ids */
  override def print(graph: LoamGraph): String = {
    val delimiter = "\n"

    def toString(parts: Iterable[String]): String = parts.mkString(delimiter)

    val storesString = toString(graph.stores.map(print(_, fully = true)))

    val toolsString = toString(graph.tools.map(print))

    val storeSourcesString = toString(graph.storeSources.map {
      case (store, source) => s"${print(store, fully = false)} <- ${print(source)}"
    })

    toString(Seq(storesString, toolsString, storeSourcesString))
  }
}
