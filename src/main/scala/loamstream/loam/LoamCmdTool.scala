package loamstream.loam

import loamstream.loam.LoamToken.{StoreRefToken, StoreToken, StringToken}
import loamstream.model.{LId, Store}
import loamstream.util.{StringUtils, ValueBox}

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object LoamCmdTool {

  type Tool = LoamCmdTool

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
    val tool = LoamCmdTool(LId.newAnonId, tokens)
    
    graphBox.mutate(_.withTool(tool))
    
    tool
  }
}

/** A command line tool specified in a Loam script */
final case class LoamCmdTool private(id: LId, tokens: Seq[LoamToken])(implicit val graphBox: ValueBox[LoamGraph])
  extends LoamTool {
  
  /** Input and output stores before any are specified using in or out */
  override def defaultStores: Set[LoamStore] = LoamToken.storesFromTokens(tokens)
}