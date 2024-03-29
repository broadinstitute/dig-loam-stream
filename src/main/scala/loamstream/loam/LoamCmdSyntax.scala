package loamstream.loam

import loamstream.util.StringUtils
import loamstream.model.LId
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.model.Store
import loamstream.conf.DynamicConfig
import loamstream.model.Tool


/**
 * @author clint
 * @author kaan
 * @author oliverr
 * 
 * Jul 31, 2019
 */
object LoamCmdSyntax extends LoamCmdSyntax

trait LoamCmdSyntax extends GraphFunctions {
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
  
  //TODO: Put this somewhere else?
  implicit final class LoamCmdToolOps[T <: Tool : CanAddPreamble](val originalTool: T) {
    def using(dotkits: String*)(implicit scriptContext: LoamScriptContext): T = {
      val prefix = {
        val useuse = "source /broad/software/scripts/useuse"
        val and = "&&"
        val reuse = "reuse -q"
        val reuses = dotkits.mkString(s"$reuse ", s" $and $reuse ", s" $and")
        
        s"$useuse $and $reuses"
      }
  
      val updatedTool = implicitly[CanAddPreamble[T]].addPreamble(prefix, originalTool)
  
      originalTool.scriptContext.projectContext.updateGraph { graph =>
        graph.updateTool(originalTool, updatedTool)
      }
  
      updatedTool
    }
  }
}
