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
object LoamCmdSyntax extends LoamCmdSyntax

trait LoamCmdSyntax {
  implicit final class StringContextWithCmd(val stringContext: StringContext) {
    /** BEWARE: This method has the implicit side-effect of modifying the graph
     *          within scriptContext via the call to addToGraph()
     */
    def cmd(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      val tool = LoamCmdTool.create(args : _*)(StringUtils.unwrapLines)(scriptContext, stringContext)

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
}
