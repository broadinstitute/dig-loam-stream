package loamstream.loam

import loamstream.LEnv
import loamstream.loam.LoamToken.{EnvToken, StoreRefToken, StoreToken, StringToken}
import loamstream.model.{LId, Store, Tool}
import loamstream.util.StringUtils

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object LoamTool {

  def createStringToken(string: String): StringToken = StringToken(StringUtils.unwrapLines(string))

  implicit class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    def cmd(args: Any*)(implicit graphBuilder: LoamGraphBuilder): LoamTool = {
      val stringPartsIter = stringContext.parts.iterator
      val argsIter = args.iterator
      var tokens: Seq[LoamToken] = Seq(createStringToken(stringPartsIter.next))
      while (stringPartsIter.hasNext) {
        val arg = argsIter.next()
        val argToken = arg match {
          case key: LEnv.KeyBase => EnvToken(key)
          case store: LoamStore => StoreToken(store)
          case storeRef: LoamStoreRef => StoreRefToken(storeRef)
          case _ => StringToken(arg.toString)
        }
        tokens :+= argToken
        tokens :+= createStringToken(stringPartsIter.next())
      }
      tokens = LoamToken.mergeStringTokens(tokens)
      LoamTool.create(tokens)
    }
  }

  def create(tokens: Seq[LoamToken])(implicit graphBuilder: LoamGraphBuilder): LoamTool =
    graphBuilder.addTool(LoamTool(LId.newAnonId), tokens)

}

case class LoamTool private(id: LId)(implicit val graphBuilder: LoamGraphBuilder) extends Tool {
  def graph: LoamGraph = graphBuilder.graph

  override def inputs: Map[LId, Store] =
    graph.toolInputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap


  override def outputs: Map[LId, Store] =
    graph.toolOutputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  def tokens: Seq[LoamToken] = graph.toolTokens(this)

}