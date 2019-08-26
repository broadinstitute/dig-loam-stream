package loamstream.compiler.v2

import loamstream.conf.DynamicConfig
import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamToken
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.model.LId
import loamstream.model.Store
import loamstream.util.StringUtils

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
trait LoamCmdToolSyntax {

  private def createStringToken(string: String)(transform: String => String): StringToken = {
    StringToken(transform(string))
  }

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

    //Associate transformations with stores when making tokens? 
    
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
  
  private[v2] def isStoreIterable(xs: Iterable[_]): Boolean = {
    xs.nonEmpty && xs.forall(_.isInstanceOf[Store])
  }
  
  def toString(tokens: Seq[LoamToken]): String = tokens.map(_.render).mkString
  
  def toToken(arg: Any): LoamToken = {
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
}
