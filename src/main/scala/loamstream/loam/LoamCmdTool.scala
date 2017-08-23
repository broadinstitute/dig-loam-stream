package loamstream.loam

import loamstream.loam.LoamToken.{MultiStoreToken, MultiToken, StoreRefToken, StoreToken, StringToken}
import loamstream.model.Tool.{AllStores, DefaultStores}
import loamstream.model.{LId, Store, Tool}
import loamstream.util.{StringUtils, TypedIterableExtractor}
import loamstream.loam.files.LoamFileManager
import loamstream.conf.DynamicConfig

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
      val tool = create(args : _*)(StringUtils.unwrapLines)(scriptContext, stringContext)

      LoamCmdTool.addToGraph(tool)

      tool
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
    scriptContext.projectContext.updateGraph { graph =>
      graph.withTool(tool, scriptContext)
    }
  }
  
  private[loam] def isHasLocationIterable(xs: Iterable[_]): Boolean = xs.nonEmpty && xs.head.isInstanceOf[HasLocation]

  private val storesIterableExtractor = TypedIterableExtractor.newFor[HasLocation]

  def toToken(arg: Any): LoamToken = arg match {
    case store: Store.Untyped => StoreToken(store)
    case storeRef: LoamStoreRef => StoreRefToken(storeRef)
    //NB: @unchecked is ok here because the check that can't be performed due to erasure is worked around by 
    //the isHasLocationIterable() guard
    case storesIterableExtractor(stores) if(stores.nonEmpty) => MultiStoreToken(stores)
    case args: Iterable[_] => MultiToken(args)
    //NB: Will throw if the DynamicConf represents a config key that's not present,
    //or a key that points to a sub-config (ie NOT a string or number)
    case conf: DynamicConfig => StringToken(conf.unpack.toString)
    case arg => StringToken(arg.toString)
  }
  
  def toString(fileManager: LoamFileManager, tokens: Seq[LoamToken]): String = {
    tokens.map(_.toString(fileManager)).mkString
  }
}

/** A command line tool specified in a Loam script */
final case class LoamCmdTool private (id: LId, tokens: Seq[LoamToken])(implicit val scriptContext: LoamScriptContext) 
    extends Tool {

  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = AllStores(LoamToken.storesFromTokens(tokens))

  /** Constructs the command line string */
  def commandLine: String = LoamCmdTool.toString(scriptContext.projectContext.fileManager, tokens)

  def using(dotkits: String*): LoamCmdTool = {
    val prefix = {
      val useuse = "source /broad/software/scripts/useuse"
      val and = "&&"
      val reuse = "reuse -q"
      val reuses = dotkits.mkString(s"$reuse ", s" $and $reuse ", s" $and")
      val openParen = "("
      s"$useuse $and $reuses $openParen"
    }

    val useToken = StringToken(prefix)
    val closeParenToken = StringToken(")")

    val updatedTool = copy(tokens = useToken +: tokens :+ closeParenToken)

    scriptContext.projectContext.updateGraph { graph =>
      graph.updateTool(this, updatedTool)
    }

    updatedTool
  }
}
