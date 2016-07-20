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
      //TODO: handle case where there are no parts (can that happen? cmd"" ?)
      val firstPart +: stringParts = stringContext.parts

      val firstToken: LoamToken = createStringToken(firstPart)
      
      val tokens: Seq[LoamToken] = firstToken +: {
        stringParts.zip(args).flatMap { case (stringPart, arg) =>
          val argToken = arg match {
            case key: LEnv.KeyBase => EnvToken(key)
            case store: LoamStore => StoreToken(store)
            case storeRef: LoamStoreRef => StoreRefToken(storeRef)
            case _ => StringToken(arg.toString)
          }
          Seq(argToken, createStringToken(stringPart))
        }
      }

      val merged = LoamToken.mergeStringTokens(tokens)
      
      LoamTool.create(merged)
    }
  }

  def create(tokens: Seq[LoamToken])(implicit graphBuilder: LoamGraphBuilder): LoamTool =
    graphBuilder.addTool(LoamTool(LId.newAnonId), tokens)

}

/** A tool specified in a Loam script */
final case class LoamTool private(id: LId)(implicit val graphBuilder: LoamGraphBuilder) extends Tool {
  /** The graph this tool is part of */
  def graph: LoamGraph = graphBuilder.graph

  /** Input stores of this tool */
  override def inputs: Map[LId, Store] =
    graph.toolInputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Output stores of this tool */
  override def outputs: Map[LId, Store] =
    graph.toolOutputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Tokens used in the tool definition, representing parts of interpolated string and embedded objects */
  def tokens: Seq[LoamToken] = graph.toolTokens(this)

  //TODO: TEST
  /** Adds input stores to this tool */
  def in(inStore: LoamStore, inStores: LoamStore*): LoamTool = {
    graphBuilder.addInputStores(this, (inStore +: inStores).toSet)
    this
  }

  //TODO: TEST
  /** Adds output stores to this tool */
  def out(outStore: LoamStore, outStores: LoamStore*): LoamTool = {
    graphBuilder.addOutputStores(this, (outStore +: outStores).toSet)
    this
  }
}