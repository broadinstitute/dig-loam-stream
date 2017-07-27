package loamstream.v2

import rx.lang.scala.Observable
import java.io.File
import loamstream.util.Files

class Runner {
  def run(context: Context): Observable[Tool.Snapshot] = {
    val tools = context.queue.runnables

    import scala.sys.process._
    
    def actuallyRun(tool: Tool): ToolState = tool match {
      case c: Command => {
        val commandLine = c.commandLine(context.state().symbols)
        
        val tempFile = File.createTempFile("loamstream", ".sh")
        
        Files.writeTo(tempFile.toPath)(commandLine)
        
        val parts = Seq("bash", tempFile.toPath.toAbsolutePath.toString)
        
        println(s"Running: '$commandLine'")
        println(s"Actually Running: '${parts.mkString(" ")}'")
        
        val result = parts.!
        
        println(s"Got '$result' from running '$commandLine'")
        
        if(result == 0) ToolState.Finished else ToolState.Failed
      }
      case t: Task[_] => {
        println(s"Performing: $t")
        
        val result = t.perform()
        
        println(s"Done performing: $t")
        
        result
      }
    }
    
    tools.map { t =>
      val newState = actuallyRun(t)
      
      t.transitionTo(newState)
      
      Tool.Snapshot(newState, t)
    }
  }
}
