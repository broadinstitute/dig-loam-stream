package loamstream

import org.scalatest.FunSuite
import loamstream.loam.LoamCmdTool
import loamstream.compiler.LoamPredef
import loamstream.loam.ops.StoreType
import LoamstreamShouldntHangTest.Pipelines
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamGraph
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineStringJob

/**
 * @author clint
 * Sep 14, 2017
 */
final class LoamstreamShouldntHangTest extends FunSuite {
  test("LS Shouldn't hang, even in the face of failed jobs") {
    val descriptor = Pipelines.someEarlyFailures
    
    import scala.concurrent.duration._
    
    val results = TestHelpers.run(descriptor.graph, 15.seconds)
    
    def jobFor(tool: LoamCmdTool): LJob = {
      results.keys.find(_.asInstanceOf[CommandLineStringJob].commandLineString == tool.commandLine).get
    }
    
    def shouldHaveFailed: LJob = jobFor(descriptor.fails)
    
    assert(results(shouldHaveFailed).isFailure === true)
  }
}

object LoamstreamShouldntHangTest {
  object Pipelines {
    final case class SomeFailures(
        graph: LoamGraph,
        fails: LoamCmdTool,
        xToY: LoamCmdTool,
        aToB: LoamCmdTool,
        bToC: LoamCmdTool,
        cToD: LoamCmdTool)
    
    val someEarlyFailures: SomeFailures = {
      implicit val scriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
      import LoamPredef._
      import LoamCmdTool._
      import StoreType.TXT
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store[TXT].at(s"$workDir/foo/bar/baz").asInput
      val storeX = store[TXT].at(s"$workDir/x.txt")
      val storeY = store[TXT].at(s"$workDir/y.txt")
      
      val storeA = store[TXT].at("src/test/resources/a.txt").asInput
      val storeB = store[TXT].at(s"$workDir/b.txt")
      val storeC = store[TXT].at(s"$workDir/c.txt")      
      val storeD = store[TXT].at(s"$workDir/d.txt")
      
      /*
       *   a.txt -> b.txt ->  c.txt -> d.txt
       *                 \
       *                  +-> x.txt -> y.txt
       *                 /
       *            nonexistent 
       */
      
      local {
        val fails = cmd"cat $nonexistent $storeB > $storeX".in(nonexistent, storeB).out(storeX)
        val xToY = cmd"cp $storeX $storeY".in(storeX).out(storeY)
        val aToB = cmd"cp $storeA $storeB".in(storeA).out(storeB)
        val bToC = cmd"cp $storeB $storeC".in(storeB).out(storeC)
        val cToD = cmd"cp $storeC $storeD".in(storeC).out(storeD)
        val graph = scriptContext.projectContext.graph
        
        SomeFailures(graph, fails, xToY, aToB, bToC, cToD)
      }
    }
  }
}
