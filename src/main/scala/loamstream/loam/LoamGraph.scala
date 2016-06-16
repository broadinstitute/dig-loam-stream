package loamstream.loam

import java.nio.file.Path

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.LoamToken.{EnvToken, StringToken}

import scala.reflect.runtime.universe.typeOf

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object LoamGraph {

  trait StoreEdge

  object StoreEdge {

    case class PathEdge(path: Path) extends StoreEdge

    case class PathKeyEdge(key: LEnv.Key[Path]) extends StoreEdge

    case class ToolEdge(tool: LoamTool) extends StoreEdge

  }

  def empty: LoamGraph = LoamGraph(Set.empty, Set.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)
}

case class LoamGraph(stores: Set[LoamStore], tools: Set[LoamTool], toolTokens: Map[LoamTool, Seq[LoamToken]],
                     toolInputs: Map[LoamTool, Set[LoamStore]],
                     toolOutputs: Map[LoamTool, Set[LoamStore]],
                     storeSources: Map[LoamStore, StoreEdge],
                     storeSinks: Map[LoamStore, Set[StoreEdge]]) {

  def withStore(store: LoamStore): LoamGraph = copy(stores = stores + store)

  def withTool(tool: LoamTool, tokens: Seq[LoamToken]): LoamGraph =
    if (!tools(tool)) {
      val toolStores = LoamToken.storesFromTokens(tokens)
      val toolInputStores = toolStores.filter(storeSources.contains)
      val toolOutputStores = toolStores -- toolInputStores
      val toolEdge = StoreEdge.ToolEdge(tool)
      val outputsWithSource = toolOutputStores.map(store => store -> toolEdge)
      val storeSinksNew =
        toolInputStores.map(store => store -> (storeSinks.getOrElse(store, Set.empty) + toolEdge))
      copy(tools = tools + tool, toolTokens = toolTokens + (tool -> tokens),
        toolInputs = toolInputs + (tool -> toolInputStores),
        toolOutputs = toolOutputs + (tool -> toolOutputStores), storeSources = storeSources ++ outputsWithSource,
        storeSinks = storeSinks ++ storeSinksNew)
    } else {
      this
    }

  def withStoreSource(store: LoamStore, source: StoreEdge): LoamGraph =
    copy(storeSources = storeSources + (store -> source))

  def withStoreSink(store: LoamStore, sink: StoreEdge): LoamGraph =
    copy(storeSinks = storeSinks + (store -> (storeSinks.getOrElse(store, Set.empty) + sink)))

  def storeProducers(store: LoamStore): Option[LoamTool] = storeSources.get(store).flatMap({
    case StoreEdge.ToolEdge(tool) => Some(tool)
    case _ => None
  })

  def storeConsumers(store: LoamStore): Set[LoamTool] = storeSinks.getOrElse(store, Set.empty).flatMap({
    case StoreEdge.ToolEdge(tool) => Some(tool)
    case _ => None
  })

  def toolsPreceding(tool: LoamTool): Set[LoamTool] =
    toolInputs.getOrElse(tool, Set.empty).flatMap(storeSources.get).collect({
      case StoreEdge.ToolEdge(toolPreceding) => toolPreceding
    })

  def toolsSucceeding(tool: LoamTool): Set[LoamTool] =
    toolOutputs.getOrElse(tool, Set.empty).flatMap(storeConsumers)

  def withEnv(env: LEnv): LoamGraph = {
    val toolTokensNew = toolTokens.mapValues({ tokens =>
      val tokensMapped = tokens.map({
        case token@EnvToken(key) if key.tpe =:= typeOf[Path] => env.get(key.asInstanceOf[LEnv.Key[Path]]) match {
          case Some(path) => StringToken(path.toString)
          case None => token
        }
        case token => token
      })
      LoamToken.mergeStringTokens(tokensMapped)
    }).view.force
    copy(toolTokens = toolTokensNew)
  }
}
