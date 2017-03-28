package loamstream.loam

import loamstream.loam.LoamToken.{StoreRefToken, StoreToken, StringToken}
import loamstream.loam.LoamTool.{AllStores, DefaultStores}
import loamstream.model.LId
import loamstream.util.StringUtils
import loamstream.model.execute.ExecutionEnvironment
import loamstream.loam.files.LoamFileManager

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object LoamCmdTool {

  def createStringToken(string: String): StringToken = StringToken(StringUtils.unwrapLines(string))

  implicit final class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    def cmd(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      //TODO: handle case where there are no parts (can that happen? cmd"" ?)
      val firstPart +: stringParts = stringContext.parts

      val firstToken: LoamToken = createStringToken(firstPart)

      val tokens: Seq[LoamToken] = firstToken +: {
        stringParts.zip(args).flatMap { case (stringPart, arg) =>
          Seq(toToken(arg), createStringToken(stringPart))
        }
      }

      val merged = LoamToken.mergeStringTokens(tokens)

      LoamCmdTool.create(merged)
    }
  }

  def create(tokens: Seq[LoamToken])(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
    val tool = LoamCmdTool(LId.newAnonId, tokens)
    
    scriptContext.projectContext.graphBox.mutate { graph =>
      graph.withTool(tool, scriptContext)
    }
    
    tool
  }
  
  def toToken(arg: Any): LoamToken = arg match {
    case store: LoamStore.Untyped => StoreToken(store)
    case storeRef: LoamStoreRef => StoreRefToken(storeRef)
    case arg => StringToken(arg.toString)
  }
  
  def toString(fileManager: LoamFileManager, tokens: Seq[LoamToken]): String = {
    tokens.map(_.toString(fileManager)).mkString
  }
}

/** A command line tool specified in a Loam script */
final case class LoamCmdTool private (id: LId, tokens: Seq[LoamToken])(implicit val scriptContext: LoamScriptContext) 
    extends LoamTool {

  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = AllStores(LoamToken.storesFromTokens(tokens))

  /** Constructs the command line string */
  def commandLine: String = LoamCmdTool.toString(scriptContext.projectContext.fileManager, tokens)
}
