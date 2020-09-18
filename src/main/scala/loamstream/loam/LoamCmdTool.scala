package loamstream.loam

import loamstream.conf.DynamicConfig
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.model.LId
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.Tool.AllStores
import loamstream.model.Tool.DefaultStores
import loamstream.util.StringUtils

/** 
 *  A command line tool specified in a Loam script
 *  @author clint
 *  @author oliverr
 *  @author kaan
 *  
 *  May 25, 2016
 */
final case class LoamCmdTool private (
    val id: LId, 
    val tokens: Seq[LoamToken])(implicit val scriptContext: LoamScriptContext) extends Tool {

  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = AllStores(LoamToken.storesFromTokens(tokens))

  /** Constructs the command line string */
  def commandLine: String = LoamCmdTool.toString(tokens)
}

object LoamCmdTool {
  def toString(tokens: Seq[LoamToken]): String = tokens.map(_.render).mkString
  
  /**
   * @param transform allows for manipulating white space,
   *                  system-dependent markers (e.g. line breaks), etc.
   *                  within a commandline or a block of embedded code
   */
  private[loam] def create(args: Any*)
                          (transform: String => String)
                          (implicit scriptContext: LoamScriptContext, stringContext: StringContext): LoamCmdTool = {

    def toStringToken(s: String) = StringToken(transform(s))
    
    //TODO: handle case where there are no parts (can that happen? cmd"" ?)
    val firstPart +: stringParts = stringContext.parts

    val firstToken: LoamToken = toStringToken(firstPart)

    //Associate transformations with stores when making tokens? 
    
    val tokens: Seq[LoamToken] = firstToken +: {
      stringParts.zip(args).flatMap { case (stringPart, arg) =>
        Seq(toToken(arg), toStringToken(stringPart))
      }
    }

    val merged = LoamToken.mergeStringTokens(tokens)

    LoamCmdTool(LId.newAnonId, merged)
  }
  
  private[loam] def toToken(arg: Any): LoamToken = {
    def isInputStore(s: Store) = s.graph.inputStores.contains(s)
    
    arg match {
      case store: Store => StoreToken(store)
      //NB: @unchecked is ok here because the check that can't be performed due to erasure is worked around by 
      //the isHasLocationIterable() guard
      case stores: Iterable[Store] @unchecked if isStoreIterable(stores) => {
        MultiStoreToken(stores)
      }
      case args: Iterable[_] => MultiToken(args)
      //NB: Will throw if the DynamicConf represents a config key that's not present,
      //or a key that points to a sub-config (ie NOT a string or number)
      case conf: DynamicConfig => StringToken(conf.unpack.toString)
      case arg => StringToken(arg.toString)
    }
  }
  
  private[loam] def isStoreIterable(xs: Iterable[_]): Boolean = {
    xs.nonEmpty && xs.forall(_.isInstanceOf[Store])
  }
  
  implicit object CanAddPreambleToLoamCmdTools extends CanAddPreamble[LoamCmdTool] {
    override def addPreamble(
        preamble: String, 
        orig: LoamCmdTool)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      
      val useToken = StringToken(s"${preamble} (")
      val closeParenToken = StringToken(")")
  
      orig.copy(tokens = useToken +: orig.tokens :+ closeParenToken)
    }
  }
}
