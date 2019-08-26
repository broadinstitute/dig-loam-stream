package loamstream.v2

import org.scalatest.FunSuite
import java.nio.file.Path
import loamstream.TestHelpers
import loamstream.util.Observables
import loamstream.util.Paths.Implicits._
import loamstream.util.Files

/**
 * @author clint
 * Aug 6, 2019
 */
final class DryRunningTest extends FunSuite {
  test("one command") {
    val (_, tools)  = dryRun(Pipelines.oneCommand)
    
    val Seq(tool) = tools
    
    assert(tool.isInstanceOf[Command])
  }
  
  test("simple scatter") {
    withPipeline(Pipelines.simpleScatter) { (outputDir, context) =>
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
      
      val tools = doDryRun(context)
    
      val firstTool +: otherTools = tools 
      
      assert(context.nameOf(firstTool) === "foo2bar")

      assert(otherTools.drop(1).map(context.nameOf).toSet === Set("baz-0.txt", "baz-1.txt", "baz-2.txt", "baz-3.txt"))
    }
  }
  
  private def dryRun(makePipeline: Path => Context): (Context, Seq[Tool]) = {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val pipeline = makePipeline(workDir)
      
      (pipeline, doDryRun(pipeline))
    }
  }
  
  def doDryRun(pipeline: Context): Seq[Tool] = {
    val dryRunner = new DryRunner
    
    import Observables.Implicits._
      
    TestHelpers.waitFor(dryRunner.toolsRun(pipeline).firstAsFuture)
  }
  
  private def withPipeline[A](makePipeline: Path => Context)(f: (Path, Context) => A): A = {
    TestHelpers.withWorkDir(getClass.getSimpleName) { outputDir =>
      val context = makePipeline(outputDir)
      
      try { 
        f(outputDir, context) 
      } finally {
        //context.finishFirstEvaluationPass()
      }
    }
  }
}
