package loamstream.v2

import org.scalatest.FunSuite
import loamstream.util.ObservableEnrichments
import loamstream.util.Futures
import loamstream.util.Files
import loamstream.TestHelpers
import java.nio.file.Path
import org.apache.commons.io.FileUtils
import loamstream.util.PathEnrichments


/**
 * @author clint
 * Jul 27, 2017
 */
final class DynamicExecutionTest extends FunSuite { self =>
  import TestHelpers.path
  import PathEnrichments.PathHelpers
  import DynamicExecutionTest.Pipelines
  
  private def withOutputDir[A](subDir: String)(f: Path => A): A = {
    val outputDir = path("target") / self.getClass.getSimpleName / subDir
    
    val asFile = outputDir.toFile
    
    FileUtils.deleteDirectory(asFile)

    assert(asFile.mkdirs())
    
    assert(asFile.exists)
    
    f(outputDir)
  }
  
  test("Simple scatter pipeline") {
    
    withOutputDir("scatter") { outputDir =>
      val foo = outputDir / "foo.txt" 
      val bar = outputDir / "bar.txt"
      val baz0 = outputDir / "baz-0.txt"
      val baz1 = outputDir / "baz-1.txt"
      val baz2 = outputDir / "baz-2.txt"
      val baz3 = outputDir / "baz-3.txt"
  
      val context = Pipelines.simpleScatter(outputDir)
    
      assert(!foo.toFile.exists)
      
      val fooContents = """| Foo
                           | Bar
                           | Baz
                           | Blerg""".stripMargin
      
      Files.writeTo(foo)(fooContents)
      
      val runner = new Runner
    
      val results = runner.run(context)
    
      import ObservableEnrichments._
    
      Futures.waitFor(results.lastAsFuture)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(Files.readFrom(foo) === Files.readFrom(bar))
      
      assert(Files.readFrom(baz0) === "0")
      assert(Files.readFrom(baz1) === "11")
      assert(Files.readFrom(baz2) === "222")
      assert(Files.readFrom(baz3) === "3333")
    }
  }
  
  test("Simple scatter-gather pipeline") {
    
    withOutputDir("scatter-gather") { outputDir =>
      val foo = outputDir / "foo.txt" 
      val bar = outputDir / "bar.txt"
      val baz0 = outputDir / "baz-0.txt"
      val baz1 = outputDir / "baz-1.txt"
      val baz2 = outputDir / "baz-2.txt"
      val baz3 = outputDir / "baz-3.txt"
      val blerg = outputDir / "blerg.txt"
  
      val context = Pipelines.scatterGather(outputDir)
    
      assert(!foo.toFile.exists)
      
      val fooContents = """| Foo
                           | Bar
                           | Baz
                           | Blerg""".stripMargin
      
      Files.writeTo(foo)(fooContents)
      
      val runner = new Runner
    
      val results = runner.run(context)
    
      import ObservableEnrichments._
    
      Futures.waitFor(results.lastAsFuture)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(Files.readFrom(foo) === Files.readFrom(bar))
      
      assert(Files.readFrom(baz0) === "0")
      assert(Files.readFrom(baz1) === "11")
      assert(Files.readFrom(baz2) === "222")
      assert(Files.readFrom(baz3) === "3333")
      
      val expectedBlergContents = """|0
                                     |11
                                     |222
                                     |3333""".stripMargin
      
      assert(Files.readFrom(blerg) === expectedBlergContents)
    }
  }
  
  test("Linear with data deps") {
    
    withOutputDir("linear-with-data-deps") { outputDir =>
      val foo = outputDir / "foo.txt" 
      val bar = outputDir / "bar.txt"
      val baz = outputDir / "baz.txt"
  
      val context = Pipelines.linearWithDataDeps(outputDir)
    
      assert(!foo.toFile.exists)
      
      val fooContents = """| Foo
                           | Bar""".stripMargin
      
      Files.writeTo(foo)(fooContents)
      
      val runner = new Runner
    
      val results = runner.run(context)
    
      import ObservableEnrichments._
    
      Futures.waitFor(results.lastAsFuture)
      
      assert(Files.readFrom(foo) === fooContents)

      val expectedBarContents = """|bar-1
                                   |bar-2""".stripMargin
      
      assert(Files.readFrom(bar).trim === expectedBarContents)
      
      val expectedBazContents = """|baz-1
                                   |baz-2
                                   |baz-3
                                   |baz-4""".stripMargin
      
      assert(Files.readFrom(baz).trim === expectedBazContents)
    }
  }
}

object DynamicExecutionTest {
  object Pipelines {
    import TestHelpers.path
    import PathEnrichments.PathHelpers
    
    def simpleScatter(outputDir: Path): Context = { 
      import V2Predef._
      
      implicit val context = new Context
      
      val foo = store(outputDir / "foo.txt")
      
      val bar = store(outputDir / "bar.txt")
      
      cmd"cp $foo $bar".in(foo).out(bar)
      
      def countLines(s: Store) = lift(Files.countLines)(s)
      
      val count = value(countLines(bar).toInt)
      
      val bazes = loop(count) { i =>
        val out = store(outputDir / s"baz-$i.txt")
        
        val content = i.toString * (i + 1)
        
        cmd"echo $content > $out".out(out)
        
        out
      }
      
      bazes.in(count)
      
      context
    }
    
    def scatterGather(outputDir: Path): Context = { 
      import V2Predef._
      
      implicit val context = new Context
      
      val foo = store(outputDir / "foo.txt")
      
      val bar = store(outputDir / "bar.txt")
      
      val blerg = store(outputDir / "blerg.txt")
      
      cmd"cp $foo $bar".in(foo).out(bar)
      
      def countLines(s: Store) = lift(Files.countLines)(s)
      
      val count = value(countLines(bar).toInt)
      
      val bazes = loop(count) { i =>
        val out = store(outputDir / s"baz-$i.txt")
        
        val content = i.toString * (i + 1)
        
        cmd"echo $content > $out".out(out)
        
        out
      }
      
      bazes.in(count)
      
      val gatherStep = bazes.map { stores => 
        cmd"cat $stores > $blerg".in(stores).out(blerg)
      }
      
      context
    }
    
    def linearWithDataDeps(outputDir: Path): Context = { 
      import V2Predef._
      
      implicit val context = new Context
      
      val foo = store(outputDir / "foo.txt")
      
      val bar = store(outputDir / "bar.txt")
      
      val baz = store(outputDir / "baz.txt")
      
      def countLines(s: Store) = {
        import scala.collection.JavaConverters._
        
        def count(p: Path) = java.nio.file.Files.lines(p).iterator.asScala.filter(_.trim.nonEmpty).size
        
        val result = lift(count)(s)
        
        println(s"$result lines in ${s.render}")
        
        result
      }
      
      val fooCount = value(countLines(foo).toInt)
      
      val barCount = value(countLines(bar).toInt)
      
      val barCountTimesTwo = barCount.map(_ * 2)
      
      cmd"""str=""
            for i in {1..$fooCount} 
            do 
              str="$${str}bar-$${i}\n" 
            done  
            echo -e "$${str}" > $bar""".in(fooCount).out(bar)
            
      cmd"""str=""
            for i in {1..$barCountTimesTwo} 
            do 
              str="$${str}baz-$${i}\n" 
            done  
            echo -e "$${str}" > $baz""".in(barCount).out(baz)
      
      context
    }
  }
}
