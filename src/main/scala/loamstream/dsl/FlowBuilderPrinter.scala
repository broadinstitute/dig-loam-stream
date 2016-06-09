package loamstream.dsl

import loamstream.LEnv
import loamstream.dsl.FlowBuilder.StoreSource
import loamstream.dsl.FlowBuilder.StoreSource.{FromPath, FromPathKey, FromTool}
import loamstream.dsl.ToolBuilder.{EnvToken, StoreToken, StringToken}
import loamstream.model.LId
import loamstream.util.SourceUtils

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 6/9/2016.
  */
case class FlowBuilderPrinter(idLength: Int) {

  def print(id: LId): String = id match {
    case LId.LNamedId(name) => name
    case LId.LAnonId(time, random) =>
      val hex = random.toHexString
      if (hex.length > 4) hex.substring(hex.length - 4) else hex
  }

  def print(tpe: Type, fully: Boolean): String =
    if (fully) SourceUtils.fullTypeName(tpe) else SourceUtils.shortTypeName(tpe)

  def print(key: LEnv.Key[_], fully: Boolean): String = s"%${print(key.id)}[${print(key.tpe, fully)}]"

  def print(store: StoreBuilder, fully: Boolean): String = s"@${print(store.id)}[${print(store.tpe, fully)}]"

  def print(tool: ToolBuilder): String = print(tool, tool.flowBuilder)

  def print(tool: ToolBuilder, flow: FlowBuilder): String = {
    val tokenString = tool.tokens.map({
      case StringToken(string) => string
      case EnvToken(key) => s"${print(key.id)}[${print(key.tpe, fully = false)}]"
      case StoreToken(store) =>
        val ioPrefix = flow.storeSources.get(store) match {
          case Some(StoreSource.FromTool(sourceTool)) if sourceTool == tool => ">"
          case _ => "<"
        }
        s"$ioPrefix${print(store, fully = false)}"
    }).mkString
    s"#${print(tool.id)}[$tokenString]"
  }

  def print(source: StoreSource): String = source match {
    case FromPath(path) => path.toString
    case FromPathKey(key) => print(key, fully = true)
    case FromTool(tool) => s"#${print(tool.id)}"
  }

  def print(flow: FlowBuilder): String = {
    val storesString = flow.stores.map(print(_, fully = true)).mkString("\n")
    val toolsString = flow.tools.map(print(_)).mkString("\n")
    val storeSourcesString = flow.storeSources.map({
      case (store, source) => s"${print(store, fully = false)} <- ${print(source)}"
    }).mkString("\n")
    Seq(storesString, toolsString, storeSourcesString).mkString("\n")
  }

}
