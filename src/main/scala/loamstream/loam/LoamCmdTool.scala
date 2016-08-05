package loamstream.loam

import loamstream.loam.LoamToken.{StoreRefToken, StoreToken, StringToken}
import loamstream.model.{LId, Store, Tool}
import loamstream.util.{StringUtils, ValueBox}

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object LoamCmdTool {

  def createStringToken(string: String): StringToken = StringToken(StringUtils.unwrapLines(string))

  implicit class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    def cmd(args: Any*)(implicit graphBox: ValueBox[LoamGraph]): LoamCmdTool = {
      //TODO: handle case where there are no parts (can that happen? cmd"" ?)
      val firstPart +: stringParts = stringContext.parts

      val firstToken: LoamToken = createStringToken(firstPart)

      val tokens: Seq[LoamToken] = firstToken +: {
        stringParts.zip(args).flatMap { case (stringPart, arg) =>
          val argToken = arg match {
            case store: LoamStore => StoreToken(store)
            case storeRef: LoamStoreRef => StoreRefToken(storeRef)
            case _ => StringToken(arg.toString)
          }
          Seq(argToken, createStringToken(stringPart))
        }
      }

      val merged = LoamToken.mergeStringTokens(tokens)

      LoamCmdTool.create(merged)
    }
  }

  def create(tokens: Seq[LoamToken])(implicit graphBox: ValueBox[LoamGraph]): LoamCmdTool = {
    val tool = LoamCmdTool(LId.newAnonId)
    graphBox(_.withTool(tool, tokens))
    tool
  }
}

/** A command line tool specified in a Loam script */
final case class LoamCmdTool private(id: LId)(implicit val graphBox: ValueBox[LoamGraph]) extends LoamTool {
  /** Input stores of this tool */
  override def inputs: Map[LId, Store] =
    graph.toolInputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Output stores of this tool */
  override def outputs: Map[LId, Store] =
    graph.toolOutputs.getOrElse(this, Set.empty).map(store => (store.id, store)).toMap

  /** Tokens used in the tool definition, representing parts of interpolated string and embedded objects */
  def tokens: Seq[LoamToken] = graph.toolTokens(this)

  /** Adds input stores to this tool */
  def in(inStore: LoamStore, inStores: LoamStore*): LoamCmdTool = {
    graphBox(_.withInputStores(this, (inStore +: inStores).toSet))
    this
  }

  /** Adds output stores to this tool */
  def out(outStore: LoamStore, outStores: LoamStore*): LoamCmdTool = {
    graphBox(_.withOutputStores(this, (outStore +: outStores).toSet))
    this
  }
}