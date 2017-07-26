package loamstream.v2

import loamstream.util.Files
import rx.lang.scala.Observable
import loamstream.util.ObservableEnrichments
import loamstream.util.Futures

object Foo extends App {
  def wireUp(): Context = {
    import V2Predef._
    
    implicit val context = new Context
    
    val foo = store("foo.txt")
    
    val bar = store("bar.txt")
    
    cmd"cp $foo $bar".in(foo).out(bar)
    
    def countLines(s: Store) = lift(Files.countLines)(s)
    
    val count = value(countLines(bar).toInt)
    
    val bazes = loop(count) { i =>
      val out = store(s"baz-$i.txt")
      
      val content = i.toString * (i + 1)
      
      cmd"echo $content > $out".out(out)
      
      out
    }
    
    bazes.in(count)
    
    context
  }
  
  def run(context: Context): Observable[ToolState] = {
    val tools = context.runnables

    import scala.sys.process._
    
    def actuallyRun(tool: Tool): ToolState = tool match {
      case c: Command => {
        val commandLine = c.commandLine(context.symbols())
        
        println(s"Running: '$commandLine'")
        
        val result = Seq("sh", "-c", commandLine).!
        
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
      
      newState
    }
  }
  
  val context = wireUp()
  
  println(context)
  
  val results = run(context)
  
  import ObservableEnrichments._
  
  Futures.waitFor(results.lastAsFuture)
}
