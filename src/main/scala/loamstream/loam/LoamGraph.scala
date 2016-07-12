package loamstream.loam

import java.nio.file.Path

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.LoamToken.{EnvToken, StringToken}

import scala.reflect.runtime.universe.typeOf

/** The graph of all Loam stores and tools and their relationships */
object LoamGraph {

  /** A connection between a store and a tool or other consumer or producer */
  trait StoreEdge {
    def withEnv(env: LEnv): StoreEdge = this
  }

  /** A connection between a store and a tool or other consumer or producer */
  object StoreEdge {

    /** A connection between a store and a path */
    case class PathEdge(path: Path) extends StoreEdge

    /** A connection between a store and a path-type environment key  */
    case class PathKeyEdge(key: LEnv.Key[Path]) extends StoreEdge {
      override def withEnv(env: LEnv): StoreEdge = env.get(key) match {
        case Some(path) => PathEdge(path)
        case _ => this
      }
    }

    /** A connection between a store and a tool */
    case class ToolEdge(tool: LoamTool) extends StoreEdge

  }

  /** An empty graph */
  def empty: LoamGraph = LoamGraph(Set.empty, Set.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)
}

/** The graph of all Loam stores and tools and their relationships */
case class LoamGraph(stores: Set[LoamStore], tools: Set[LoamTool], toolTokens: Map[LoamTool, Seq[LoamToken]],
                     toolInputs: Map[LoamTool, Set[LoamStore]],
                     toolOutputs: Map[LoamTool, Set[LoamStore]],
                     storeSources: Map[LoamStore, StoreEdge],
                     storeSinks: Map[LoamStore, Set[StoreEdge]]) {

  /** Returns graph with store added */
  def withStore(store: LoamStore): LoamGraph = copy(stores = stores + store)

  /** Returns graph with tool added */
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

  /** Returns graph with store source (tool or file) added */
  def withStoreSource(store: LoamStore, source: StoreEdge): LoamGraph =
    copy(storeSources = storeSources + (store -> source))

  /** Returns graph with store sink (tool or file) added */
  def withStoreSink(store: LoamStore, sink: StoreEdge): LoamGraph =
    copy(storeSinks = storeSinks + (store -> (storeSinks.getOrElse(store, Set.empty) + sink)))

  /** Returns graph with store producer (tool) added */
  def storeProducers(store: LoamStore): Option[LoamTool] = storeSources.get(store).flatMap({
    case StoreEdge.ToolEdge(tool) => Some(tool)
    case _ => None
  })

  /** Returns graph with store consumer (tool) added */
  def storeConsumers(store: LoamStore): Set[LoamTool] = storeSinks.getOrElse(store, Set.empty).flatMap({
    case StoreEdge.ToolEdge(tool) => Some(tool)
    case _ => None
  })

  /** Tools that produce a store consumed by this tool */
  def toolsPreceding(tool: LoamTool): Set[LoamTool] =
    toolInputs.getOrElse(tool, Set.empty).flatMap(storeSources.get).collect({
      case StoreEdge.ToolEdge(toolPreceding) => toolPreceding
    })

  /** Tools that consume a store produced by this tool */
  def toolsSucceeding(tool: LoamTool): Set[LoamTool] =
    toolOutputs.getOrElse(tool, Set.empty).flatMap(storeConsumers)

  /** All tools with no preceeding tools */
  def initialTools: Set[LoamTool] = tools.filter(toolsPreceding(_).isEmpty)

  /** All tools with no succeeding tools */
  def finalTools: Set[LoamTool] = tools.filter(toolsSucceeding(_).isEmpty)

  /** Returns graph with environment bindings applied */
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
    val storeSourcesNew = storeSources.mapValues(_.withEnv(env)).view.force
    val storeSinksNew = storeSinks.mapValues(_.map(_.withEnv(env))).view.force
    copy(toolTokens = toolTokensNew, storeSources = storeSourcesNew, storeSinks = storeSinksNew)
  }

  /** Optionally the path associated with a store */
  def pathOpt(store: LoamStore): Option[Path] = {
    storeSources.get(store) match {
      case Some(StoreEdge.PathEdge(path)) => Some(path)
      case _ => storeSinks.getOrElse(store, Set.empty).collect({ case StoreEdge.PathEdge(path) => path }).headOption
    }
  }

  /** Optionally, the work directory of a tool (currently always none) */
  def workDirOpt(tool: LoamTool): Option[Path] = None // TODO

  /** Ranks for all tools: zero for final tools; for all others one plus maximum of rank of succeeding tools */
  def ranks: Map[LoamTool, Int] = {
    var ranks : Map[LoamTool, Int] = tools.map(tool => (tool, 0)).toMap
    var notDoneYet = true
    while(notDoneYet) {
      notDoneYet = false
      for(tool <- tools) {
        val newRankEstimate = (toolsSucceeding(tool).map(ranks.getOrElse(_, 0)) + 0).max
        if(newRankEstimate != ranks(tool)) {
          notDoneYet = true
          ranks += (tool -> newRankEstimate)
        }
      }
    }
    ranks
  }

}
