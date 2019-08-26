package loamstream.v2

import org.scalatest.FunSuite
import loamstream.util.Observables.Implicits._
import loamstream.util.Futures
import loamstream.util.Files
import loamstream.TestHelpers
import java.nio.file.Path
import org.apache.commons.io.FileUtils
import loamstream.util.Paths.Implicits._


/**
 * @author clint
 * Jul 27, 2017
 */
final class DynamicExecutionTest extends FunSuite { self =>
  import TestHelpers.path
  
  test("One command") {
    
    withPipeline("one-command", Pipelines.oneCommand) { (outputDir, context) =>
      val foo = outputDir / "foo.txt" 
      val bar = outputDir / "bar.txt"
    
      assert(!foo.exists)
      assert(!bar.exists)
      
      val fooContents = """| Foo
                           | Bar
                           | Baz
                           | Blerg""".stripMargin
      
      Files.writeTo(foo)(fooContents)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(!bar.exists)
      
      run(context)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(Files.readFrom(foo) === Files.readFrom(bar))
    }
  }
  
  test("2-step linear") {
    
    withOutputDir("2-step-linear") { outputDir =>
      val fooPath = outputDir / "foo.txt" 
      val barPath = outputDir / "bar.txt"
      val bazPath = outputDir / "baz.txt"
  
      import V2Predef._
      
      implicit val context: Context = new Context
      
      val foo = store(fooPath)
      
      val bar = store(barPath)
      
      val baz = store(bazPath)
      
      val foo2Bar = cmd"cp $foo $bar".in(foo).out(bar).asInstanceOf[Command]
      val bar2Baz = cmd"cp $bar $baz".in(bar).out(baz).asInstanceOf[Command]
      
      context.finishFirstEvaluationPass()
      
      assert(!fooPath.exists)
      assert(!barPath.exists)
      assert(!bazPath.exists)
      
      val fooContents = """| blah1
                           | blah2""".stripMargin
      
      Files.writeTo(fooPath)(fooContents)
      
      assert(Files.readFrom(fooPath) === fooContents)
      assert(!barPath.exists)
      assert(!bazPath.exists)
      
      val results = run(context)
      
      assert(Files.readFrom(fooPath) === fooContents)
      assert(Files.readFrom(barPath) === fooContents)
      assert(Files.readFrom(bazPath) === fooContents)
      
      assert(results.forall(_.state.isFinished))
      
      {
        implicit val symbols = context.state().symbols
      
        def commandLine(t: Tool) = t.asInstanceOf[Command].commandLine(symbols)
      
        assert(results.map(_.tool).map(commandLine) === Seq(foo2Bar.commandLine, bar2Baz.commandLine))
      }
    }
  }
  
  test("3-step linear") {
    
    withOutputDir("3-step-linear") { outputDir =>
      val fooPath = outputDir / "foo.txt" 
      val barPath = outputDir / "bar.txt"
      val bazPath = outputDir / "baz.txt"
      val blergPath = outputDir / "blerg.txt"
  
      import V2Predef._
      
      implicit val context: Context = new Context
      
      val foo = store(fooPath)
      
      val bar = store(barPath)
      
      val baz = store(bazPath)
      
      val blerg = store(blergPath)
      
      val foo2Bar = cmd"cp $foo $bar".in(foo).out(bar).asInstanceOf[Command]
      val bar2Baz = cmd"cp $bar $baz".in(bar).out(baz).asInstanceOf[Command]
      val baz2Blerg = cmd"cp $baz $blerg".in(baz).out(blerg).asInstanceOf[Command]
      
      assert(!fooPath.exists)
      assert(!barPath.exists)
      assert(!bazPath.exists)
      assert(!blergPath.exists)
      
      val fooContents = """| blah1
                           | blah2""".stripMargin
      
      Files.writeTo(fooPath)(fooContents)
      
      assert(Files.readFrom(fooPath) === fooContents)
      assert(!barPath.exists)
      assert(!bazPath.exists)
      assert(!blergPath.exists)
      
      val results = run(context)
      
      assert(Files.readFrom(fooPath) === fooContents)
      assert(Files.readFrom(barPath) === fooContents)
      assert(Files.readFrom(bazPath) === fooContents)
      assert(Files.readFrom(blergPath) === fooContents)
      
      assert(results.forall(_.state.isFinished))
      
      {
        implicit val symbols = context.state().symbols
      
        def commandLine(t: Tool) = t.asInstanceOf[Command].commandLine(symbols)
      
        assert(results.map(_.tool).map(commandLine) === Seq(foo2Bar.commandLine, bar2Baz.commandLine, baz2Blerg.commandLine))
      }
    }
  }
  
  test("Simple scatter pipeline") {
    withPipeline("scatter", Pipelines.simpleScatter) { (outputDir, context) =>
      val foo = outputDir / "foo.txt" 
      val bar = outputDir / "bar.txt"
      val baz0 = outputDir / "baz-0.txt"
      val baz1 = outputDir / "baz-1.txt"
      val baz2 = outputDir / "baz-2.txt"
      val baz3 = outputDir / "baz-3.txt"
  
      assert(!foo.exists)
      assert(!bar.exists)
      assert(!baz0.exists)
      assert(!baz1.exists)
      assert(!baz2.exists)
      assert(!baz3.exists)
      
      val fooContents = """| Foo
                           | Bar
                           | Baz
                           | Blerg""".stripMargin
      
      Files.writeTo(foo)(fooContents)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(!bar.exists)
      assert(!baz0.exists)
      assert(!baz1.exists)
      assert(!baz2.exists)
      assert(!baz3.exists)
      
      run(context)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(Files.readFrom(foo) === Files.readFrom(bar))
      
      assert(Files.readFrom(baz0).trim === "0")
      assert(Files.readFrom(baz1).trim === "11")
      assert(Files.readFrom(baz2).trim === "222")
      assert(Files.readFrom(baz3).trim === "3333")
    }
  }
  
  test("Simple scatter-gather pipeline") {
    
    withPipeline("scatter-gather", Pipelines.scatterGather) { (outputDir, context) =>
      val foo = outputDir / "foo.txt" 
      val bar = outputDir / "bar.txt"
      val baz0 = outputDir / "baz-0.txt"
      val baz1 = outputDir / "baz-1.txt"
      val baz2 = outputDir / "baz-2.txt"
      val baz3 = outputDir / "baz-3.txt"
      val blerg = outputDir / "blerg.txt"
  
      assert(!foo.exists)
      assert(!bar.exists)
      assert(!baz0.exists)
      assert(!baz1.exists)
      assert(!baz2.exists)
      assert(!baz3.exists)
      assert(!blerg.exists)
      
      val fooContents = """| Foo
                           | Bar
                           | Baz
                           | Blerg""".stripMargin
      
      Files.writeTo(foo)(fooContents)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(!bar.exists)
      assert(!baz0.exists)
      assert(!baz1.exists)
      assert(!baz2.exists)
      assert(!baz3.exists)
      assert(!blerg.exists)
      
      run(context)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(Files.readFrom(foo) === Files.readFrom(bar))
      
      assert(Files.readFrom(baz0).trim === "0")
      assert(Files.readFrom(baz1).trim === "11")
      assert(Files.readFrom(baz2).trim === "222")
      assert(Files.readFrom(baz3).trim === "3333")
      
      val expectedBlergContents = """|0
                                     |11
                                     |222
                                     |3333""".stripMargin
      
      assert(Files.readFrom(blerg).trim === expectedBlergContents)
    }
  }
  
  test("Linear with data deps") {
    
    withPipeline("linear-with-data-deps", Pipelines.linearWithDataDeps) { (outputDir, context) =>
      val foo = outputDir / "foo.txt" 
      val bar = outputDir / "bar.txt"
      val baz = outputDir / "baz.txt"
    
      assert(!foo.exists)
      assert(!bar.exists)
      assert(!baz.exists)
      
      val fooContents = """| Foo
                           | Bar""".stripMargin
      
      Files.writeTo(foo)(fooContents)
      
      assert(Files.readFrom(foo) === fooContents)
      assert(!bar.exists)
      assert(!baz.exists)
      
      run(context)
      
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
  
  private def withOutputDir[A](subDir: String)(f: Path => A): A = {
    val outputDir = path("target") / self.getClass.getSimpleName / subDir
    
    val asFile = outputDir.toFile
    
    FileUtils.deleteDirectory(asFile)

    assert(asFile.mkdirs())
    
    assert(asFile.exists)
    
    f(outputDir)
  }
  
  private def withPipeline[A](subDir: String, makePipeline: Path => Context)(f: (Path, Context) => A): A = {
    withOutputDir(subDir) { outputDir =>
      val context = makePipeline(outputDir)
      
      try { 
        f(outputDir, context) 
      } finally {
        //context.finishFirstEvaluationPass()
      }
    }
  }
  
  private def run(context: Context): Seq[Tool.Snapshot] = {
    val runner = new RealRunner
    
    val results = runner.run(context)
    
    TestHelpers.waitFor(results.to[Seq].firstAsFuture)
  }
}

object DynamicExecutionTest {
  
}
