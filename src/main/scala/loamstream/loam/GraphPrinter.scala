package loamstream.loam

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.LoamGraph.StoreEdge.{PathEdge, PathKeyEdge, ToolEdge}
import loamstream.loam.LoamToken.{EnvToken, StoreToken, StringToken}
import loamstream.model.LId
import loamstream.util.SourceUtils

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 6/9/2016.
  */
final case class GraphPrinter(idLength: Int) {

  def print(id: LId): String = id.name

  def print(tpe: Type, fully: Boolean): String =
    if (fully) SourceUtils.fullTypeName(tpe) else SourceUtils.shortTypeName(tpe)

  def print(key: LEnv.Key[_], fully: Boolean): String = s"%${print(key.id)}[${print(key.tpe, fully)}]"

  def print(store: LoamStore, fully: Boolean): String = s"@${print(store.id)}[${print(store.sig.tpe, fully)}]"

  def print(tool: LoamTool): String = print(tool, tool.graphBuilder.graph)

  def print(tool: LoamTool, graph: LoamGraph): String = {
    val tokenString = graph.toolTokens.getOrElse(tool, Seq.empty).map({
      case StringToken(string) => string
      case EnvToken(key) => s"${print(key.id)}[${print(key.tpe, fully = false)}]"
      case StoreToken(store) =>
        val ioPrefix = graph.storeSources.get(store) match {
          case Some(StoreEdge.ToolEdge(sourceTool)) if sourceTool == tool => ">"
          case _ => "<"
        }
        s"$ioPrefix${print(store, fully = false)}"
    }).mkString
    s"#${print(tool.id)}[$tokenString]"
  }

  def print(source: StoreEdge): String = source match {
    case PathEdge(path) => path.toString
    case PathKeyEdge(key) => print(key, fully = true)
    case ToolEdge(tool) => s"#${print(tool.id)}"
  }

  def print(graph: LoamGraph): String = {
    val storesString = graph.stores.map(print(_, fully = true)).mkString("\n")
    val toolsString = graph.tools.map(print(_)).mkString("\n")
    val storeSourcesString = graph.storeSources.map({
      case (store, source) => s"${print(store, fully = false)} <- ${print(source)}"
    }).mkString("\n")
    Seq(storesString, toolsString, storeSourcesString).mkString("\n")
  }

}
