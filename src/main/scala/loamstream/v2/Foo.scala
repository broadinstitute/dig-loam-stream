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
  
  val context = wireUp()
  
  val runner = new Runner
  
  println(context)
  
  val results = runner.run(context)
  
  import ObservableEnrichments._
  
  Futures.waitFor(results.lastAsFuture)
}
