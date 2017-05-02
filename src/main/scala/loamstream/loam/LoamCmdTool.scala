package loamstream.loam

import loamstream.loam.LoamToken.{StoreRefToken, StoreToken, StringToken}
import loamstream.loam.LoamTool.{AllStores, DefaultStores}
import loamstream.model.LId
import loamstream.util.StringUtils
import loamstream.loam.files.LoamFileManager

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object LoamCmdTool {

  private def createStringToken(string: String)(transform: String => String): StringToken = {
    StringToken(transform(string))
  }

  implicit final class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    /** BEWARE: This method has the implicit side-effect of modifying the graph
     *          within scriptContext via the call to addToGraph()
     */
    def cmd(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      create(args : _*)(StringUtils.unwrapLines)(scriptContext, stringContext)
    }
  }

  /**
   * @param transform allows for manipulating white space,
   *                  system-dependent markers (e.g. line breaks), etc.
   *                  within a commandline or a block of embedded code
   */
  def create(args: Any*)(transform: String => String)
            (implicit scriptContext: LoamScriptContext, stringContext: StringContext): LoamCmdTool = {
    //TODO: handle case where there are no parts (can that happen? cmd"" ?)
    val firstPart +: stringParts = stringContext.parts

    val firstToken: LoamToken = createStringToken(firstPart)(transform)

    val tokens: Seq[LoamToken] = firstToken +: {
      stringParts.zip(args).flatMap { case (stringPart, arg) =>
        Seq(toToken(arg), createStringToken(stringPart)(transform))
      }
    }

    val merged = LoamToken.mergeStringTokens(tokens)

    LoamCmdTool(LId.newAnonId, merged)
  }

  /** BEWARE: This method has the side-effect of modifying the graph within scriptContext */
  private def addToGraph(tool: LoamCmdTool)(implicit scriptContext: LoamScriptContext): Unit = {
    scriptContext.projectContext.graphBox.mutate { graph =>
      graph.withTool(tool, scriptContext)
    }
  }
  
  private def toToken(arg: Any): LoamToken = arg match {
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

  def use(dotkit: String): LoamCmdTool = {
    val prefix = s"reuse -q $dotkit && "
    copy(tokens = StringToken(prefix) +: tokens)
  }

  def build: Unit = LoamCmdTool.addToGraph(this)
}
