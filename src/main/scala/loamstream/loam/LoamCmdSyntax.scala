package loamstream.loam

import loamstream.util.StringUtils
import loamstream.model.LId
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.model.Store
import loamstream.conf.DynamicConfig


/**
 * @author clint
 * @author kaan
 * @author oliverr
 * 
 * Jul 31, 2019
 */
trait LoamCmdSyntax {
  implicit final class StringContextWithCmd(val stringContext: StringContext) {
    /** BEWARE: This method has the implicit side-effect of modifying the graph
     *          within scriptContext via the call to addToGraph()
     */
    def cmd(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      val tool = create(args : _*)(StringUtils.unwrapLines)(scriptContext, stringContext)

      addToGraph(tool)

      tool
    }
  }
  
  implicit final class LoamCmdToolOps(val originalTool: LoamCmdTool) {
    def using(dotkits: String*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
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
  
      val updatedTool = originalTool.copy(tokens = useToken +: originalTool.tokens :+ closeParenToken)
  
      originalTool.scriptContext.projectContext.updateGraph { graph =>
        graph.updateTool(originalTool, updatedTool)
      }
  
      updatedTool
    }
  }
  
  /** BEWARE: This method has the side-effect of modifying the graph within scriptContext */
  private def addToGraph(tool: LoamCmdTool)(implicit scriptContext: LoamScriptContext): Unit = {
    scriptContext.projectContext.updateGraph { graph =>
      graph.withTool(tool, scriptContext)
    }
  }
  
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
}
