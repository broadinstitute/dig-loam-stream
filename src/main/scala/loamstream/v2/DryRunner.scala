package loamstream.v2

import loamstream.util.ValueBox
import rx.lang.scala.Observable

final class DryRunner extends Runner {
  private[this] val toolsRunBox: ValueBox[Seq[Tool]] = ValueBox(Nil)
  
  def toolsRun(context: Context): Observable[Seq[Tool]] = run(context).map(_ => toolsRunBox.value.reverse)
  
  override def runTool(context: Context)(tool: Tool): ToolState = {
    toolsRunBox.mutate(tool +: _)
    
    tool match {
      case c: Command => {
        val commandLine = c.commandLine(context.state().symbols)
        
        println(s"DRY RUN: Would have run command line: '$commandLine'")
        
        ToolState.Finished
      }
      case t: Task[_] => {
        println(s"DRY RUN: Performing: $t")
        
        val result = t.perform()
        
        println(s"DRY RUN: Done performing: $t")
        
        result
      }
    }
  }
}

