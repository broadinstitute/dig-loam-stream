package loamstream.loam

import loamstream.loam.LoamToken.{StoreRefToken, StoreToken, StringToken}
import loamstream.model.{LId, Store, Tool}
import loamstream.util.code.SourceUtils

import scala.reflect.runtime.universe.Type
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken

/** Prints LoamGraph for educational and debugging purposes exposing ids */
final case class GraphPrinterById(idLength: Int) extends GraphPrinter {

  /** Prints id */
  def print(id: LId): String = id.name

  /** Prints id */
  def print(tpe: Type, fully: Boolean): String =
  if (fully) SourceUtils.fullTypeName(tpe) else SourceUtils.shortTypeName(tpe)

  /** Prints store */
  def print(store: Store, fully: Boolean): String = s"@${print(store.id)}"

  /** Prints HasLocation **/
  def print(hasLocation: HasLocation, fully: Boolean): String = hasLocation match {
    case store: Store => print(store, fully)
    case storeRef: LoamStoreRef => print(storeRef.store, fully)
  }

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
  def printIoPrefix(tool: LoamCmdTool, hasLocation: HasLocation, graph: LoamGraph): String = {
    val store = hasLocation match {
      case store: Store => store
      case storeRef: LoamStoreRef => storeRef.store
    }
    graph.storeProducers.get(store) match {
      case Some(producer) if producer == tool => ">"
      case _ => "<"
    }
  }

  /** Prints tool */
  def print(tool: LoamCmdTool, graph: LoamGraph): String = {
    def hasLocationToString(hasLocation: HasLocation): String = {
      val ioPrefix = printIoPrefix(tool, hasLocation, graph)
      
      s"$ioPrefix${print(hasLocation, fully = false)}"
    }
    
    def tokenToString(token: LoamToken): String = token match {
      case StringToken(string) => string
      case StoreRefToken(ref) => hasLocationToString(ref)
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

    val storeLocationsString = toString(graph.storeLocations.map {
      case (store: HasLocation, location) => s"${print(store, fully = false)} <- ${location.toString}"
    })

    val storeProducersString = toString(graph.storeProducers.map {
      case (store: HasLocation, producer: LoamCmdTool) => s"${print(store, fully = false)} <- ${print(producer, graph)}"
      case tuple => throw new Exception(s"We don't know how to stringify non-LoamCmdTools: $tuple")
    })

    toString(Seq(storesString, toolsString, storeLocationsString, storeProducersString))
  }
}
