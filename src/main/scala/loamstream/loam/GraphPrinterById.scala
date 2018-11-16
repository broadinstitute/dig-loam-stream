package loamstream.loam

import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.model.LId
import loamstream.model.Store
import loamstream.model.Tool
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
  def print(store: Store, fully: Boolean): String = s"@${print(store.id)}"

  /** Prints cmd tool */
  def print(tool: Tool): String = tool match {
    case cmdTool: LoamCmdTool => print(cmdTool)
  }

  /** Prints cmd tool */
  def print(tool: LoamCmdTool): String = print(tool, tool.graph)

  /** Prints prefix symbol to distinguish input and output stores */
  def printIoPrefix(tool: LoamCmdTool, store: Store, graph: LoamGraph): String = {
    graph.storeProducers.get(store) match {
      case Some(producer) if producer == tool => ">"
      case _ => "<"
    }
  }

  /** Prints tool */
  def print(tool: LoamCmdTool, graph: LoamGraph): String = {
    def hasLocationToString(store: Store): String = {
      val ioPrefix = printIoPrefix(tool, store, graph)
      
      s"$ioPrefix${print(store, fully = false)}"
    }
    
    def tokenToString(token: LoamToken): String = token match {
      case StringToken(string) => string
      case StoreToken(store) => hasLocationToString(store)
      case MultiStoreToken(stores) => stores.map(hasLocationToString).mkString(" ")
      case MultiToken(things) => things.map(_.toString).mkString(" ")
    }

    val tokenString = tool.tokens.map(tokenToString).mkString

    s"#${print(tool.id)}[$tokenString]"
  }

  /** Prints LoamGraph for educational and debugging purposes exposing ids */
  override def print(graph: LoamGraph): String = {
    val delimiter = "\n"

    def toString(parts: Iterable[String]): String = parts.mkString(delimiter)

    val storesString = toString(graph.stores.map(print(_, fully = true)))

    val toolsString = toString(graph.tools.map(print))

    val storeProducersString = toString(graph.storeProducers.toMap.map {
      case (store: Store, producer: LoamCmdTool) => s"${print(store, fully = false)} <- ${print(producer, graph)}"
      case tuple => throw new Exception(s"We don't know how to stringify non-LoamCmdTools: $tuple")
    })

    toString(Seq(storesString, toolsString, storeProducersString))
  }
}
