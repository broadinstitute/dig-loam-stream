package loamstream.v2

import loamstream.util.Files
import java.io.File

final class RealRunner extends Runner {
  override def runTool(context: Context)(tool: Tool): ToolState = tool match {
    case c: Command => {
      val commandLine = c.commandLine(context.state().symbols)
      
      val tempFile = File.createTempFile("loamstream", ".sh")
      
      Files.writeTo(tempFile.toPath)(commandLine)
      
      val parts = Seq("bash", tempFile.toPath.toAbsolutePath.toString)
      
      println(s"Running: '$commandLine'")
      println(s"Actually Running: '${parts.mkString(" ")}'")
      
      import scala.sys.process._
      
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
}

/*
val commandLine = c.commandLine(context.state().symbols)
      
val processBuilder = BashScript.fromCommandLineString(commandLine).processBuilder(Paths.get("."))

println(s"Running command line: '$commandLine'")

val result = processBuilder.!

println(s"Got '$result' from running '$commandLine'")

if(result == 0) ToolState.Finished else ToolState.Failed
*/
