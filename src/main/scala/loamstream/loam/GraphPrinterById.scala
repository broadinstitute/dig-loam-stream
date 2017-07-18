package loamstream.loam

import loamstream.loam.LoamToken.{StoreRefToken, StoreToken, StringToken}
import loamstream.model.{LId, Store, Tool}
import loamstream.util.code.SourceUtils

import scala.reflect.runtime.universe.Type

/** Prints LoamGraph for educational and debugging purposes exposing ids */
final case class GraphPrinterById(idLength: Int) extends GraphPrinter {

  /** Prints id */
  def print(id: LId): String = id.name

  /** Prints id */
  def print(tpe: Type, fully: Boolean): String =
  if (fully) SourceUtils.fullTypeName(tpe) else SourceUtils.shortTypeName(tpe)

  /** Prints store */
  def print(store: Store.Untyped, fully: Boolean): String = s"@${print(store.id)}[${print(store.sig.tpe, fully)}]"

  /** Prints cmd tool */
  def print(tool: Tool): String = tool match {
    case cmdTool: LoamCmdTool => print(cmdTool)
    case nativeTool: LoamNativeTool[_] => print(nativeTool)
  }

  /** Prints cmd tool */
  def print(tool: LoamCmdTool): String = print(tool, tool.graph)

  /** Prints cmd tool */
  def print[T](tool: LoamNativeTool[T]): String = "[native tool]"

  /** Prints prefix symbol to distinguish input and output stores */
  def printIoPrefix(tool: LoamCmdTool, store: Store.Untyped, graph: LoamGraph): String = {
    graph.storeProducers.get(store) match {
      case Some(producer) if producer == tool => ">"
      case _ => "<"
    }
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

  /** Prints LoamGraph for educational and debugging purposes exposing ids */
  override def print(graph: LoamGraph): String = {
    val delimiter = "\n"

    def toString(parts: Iterable[String]): String = parts.mkString(delimiter)

    val storesString = toString(graph.stores.map(print(_, fully = true)))

    val toolsString = toString(graph.tools.map(print))

    val storeLocationsString = toString(graph.storeLocations.map {
      case (store, location) => s"${print(store, fully = false)} <- ${location.toString}"
    })

    val storeProducersString = toString(graph.storeProducers.map {
      case (store, producer: LoamCmdTool) => s"${print(store, fully = false)} <- ${print(producer, graph)}"
    })

    toString(Seq(storesString, toolsString, storeLocationsString, storeProducersString))
  }
}
