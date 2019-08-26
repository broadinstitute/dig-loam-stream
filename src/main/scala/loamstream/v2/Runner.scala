package loamstream.v2

import rx.lang.scala.Observable

trait Runner {
  def run(context: Context): Observable[Tool.Snapshot] = {
    context.queue.runnables.map { t =>
      val newState = runTool(context)(t)
  
      t.transitionTo(newState)
  
      Tool.Snapshot(newState, t)
    }
  }
  
  def runTool(context: Context)(tool: Tool): ToolState 
}
