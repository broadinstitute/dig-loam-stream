package loamstream

import scala.concurrent.duration.Duration

import org.scalatest.FunSuite

import LoamstreamShouldntHangTest.Pipelines
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamGraph
import loamstream.loam.LoamScriptContext
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob


/**
 * @author clint
 * Sep 14, 2017
 */
final class LoamstreamShouldntHangTest extends FunSuite {
  
  private def getJobFor(results: Map[LJob, Execution])(tool: LoamCmdTool): LJob = {
    def asCLJ(j: LJob): CommandLineJob = j.asInstanceOf[CommandLineJob]
    
    results.keys.map(asCLJ).find(_.commandLineString == tool.commandLine).get
  }
  
  private def doTest(descriptor: Pipelines.Descriptor): Unit = {
    //NB: Hard-wired timeouts like this always increase the risk of spurious failures, if
    //tests take too long due to, say, the Jenkins server being under load.  However, this
    //timeout is long enough, and these pipelines simple enough, that that risk is very low.
    val timeout: Duration = {
      import scala.concurrent.duration._
      
      60.seconds
    }
    
    val results = TestHelpers.run(descriptor.graph, timeout)
    
    def jobFor(tool: LoamCmdTool): LJob = getJobFor(results)(tool)
    
    def shouldHaveFailed: LJob = jobFor(descriptor.fails)
    
    assert(results(shouldHaveFailed).isFailure === true)
    
    def isAcceptable(e: Execution): Boolean = e.status.isSuccess || e.status.isCouldNotStart
    
    val shouldHaveWorkedExecutions = (results - shouldHaveFailed).values
    
    assert(shouldHaveWorkedExecutions.forall(isAcceptable))
  }
  
  //NB: This reliably triggered the hanging bug
  test("LS Shouldn't hang, even in the face of failed jobs") {
    doTest(Pipelines.someEarlyFailures)
  }
  
  test("LS Shouldn't hang when running only one failed job") {
    doTest(Pipelines.onlyOneFailure)
  }
  
  test("LS Shouldn't hang when running a 2-step pipeline where the first step fails") {
    doTest(Pipelines.twoStepsFirstFails)
  }
  
  test("LS Shouldn't hang when running a 2-step pipeline where the second step fails") {
    doTest(Pipelines.twoStepsSecondFails)
  }
  
  //NB: This reliably triggered the hanging bug, and is the most minimal way to do so
  test("LS Shouldn't hang when running a 3-step pipeline where 2 jobs that should work share a dep that fails") {
    doTest(Pipelines.threeStepsMutualDepFails)
  }
}

object LoamstreamShouldntHangTest {
  
  object Pipelines {
    trait Descriptor {
      def graph: LoamGraph
      def fails: LoamCmdTool
    }
    
    final case class OnlyOneFailure(graph: LoamGraph, fails: LoamCmdTool) extends Descriptor
    
    final case class TwoStepsOneFails(graph: LoamGraph, fails: LoamCmdTool, shouldWork: LoamCmdTool) extends Descriptor
    
    final case class SomeFailures(
        graph: LoamGraph,
        fails: LoamCmdTool,
        xToY: LoamCmdTool,
        aToB: LoamCmdTool,
        bToC: LoamCmdTool,
        cToD: LoamCmdTool) extends Descriptor
    
    final case class ThreeStepsMutualDepFails(
        graph: LoamGraph, 
        fails: LoamCmdTool,
        shouldWork0: LoamCmdTool,
        shouldWork1: LoamCmdTool) extends Descriptor
        
    /*
     * As stores: 
     * 
     *   nonexistent --> storeX
     *   
     * As jobs:
     * 
     *   willFail
     */
    val onlyOneFailure: OnlyOneFailure = {
      implicit val scriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
      import LoamPredef._
      import LoamCmdTool._
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store.at(s"$workDir/foo/bar/baz").asInput
      val storeX = store.at(s"$workDir/x.txt")
      
      local {
        val fails = cmd"cp $nonexistent $storeX"(in = Seq(nonexistent), out = Seq(storeX))
        
        OnlyOneFailure(scriptContext.projectContext.graph, fails)
      }
    }
    
    /*
     * As stores: 
     * 
     *   nonexistent --> storeX --> storeY
     *   
     * As jobs:
     * 
     *   willFail --> wouldOtherwiseWork 
     */
    val twoStepsFirstFails: TwoStepsOneFails = {
      implicit val scriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
      import LoamPredef._
      import LoamCmdTool._
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store.at(s"$workDir/foo/bar/baz").asInput
      val storeX = store.at(s"$workDir/x.txt")
      val storeY = store.at(s"$workDir/y.txt")
      
      local {
        val fails = cmd"cp $nonexistent $storeX"(in = Seq(nonexistent), out = Seq(storeX))
        val shouldWork = cmd"cp $storeX $storeY"(in = Seq(storeX), out = Seq(storeY))
        
        TwoStepsOneFails(scriptContext.projectContext.graph, fails = fails, shouldWork = shouldWork)
      }
    }
    
    /*
     * As stores: 
     * 
     *   storeA --> storeB --> cantBeWrittenTo
     *   
     * As jobs:
     * 
     *   shouldWork --> willFail  
     */
    val twoStepsSecondFails: TwoStepsOneFails = {
      implicit val scriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
      import LoamPredef._
      import LoamCmdTool._
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val storeA = store.at("src/test/resources/a.txt").asInput
      val storeB = store.at(s"$workDir/b.txt")
      val workDirStore = store.at(workDir)
      
      local {
        val shouldWork = cmd"cp $storeA $storeB"(in = Seq(storeA), out = Seq(storeB))
        //NB: Fails since cat/bash won't write a file over a directory
        val fails = cmd"cat $storeB > $workDirStore"(in = Seq(storeB), out = Seq(workDirStore))
        
        TwoStepsOneFails(scriptContext.projectContext.graph, fails = fails, shouldWork = shouldWork)
      }
    }
    
    /*
     * As stores:
     *                          +--> storeY
     *                         /
     *   nonexistent --> storeX
     *                         \
     *                          +--> storeZ
     * As jobs:   
     *            +--> shouldWork0                      
     *           /
     *   willFail 
     *           \
     *            +--> shouldWork1
     */
    val threeStepsMutualDepFails: ThreeStepsMutualDepFails = {
      implicit val scriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
      import LoamPredef._
      import LoamCmdTool._
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store.at(s"$workDir/asdf/asdf")
      val storeX = store.at(s"$workDir/x.txt")
      val storeY = store.at(s"$workDir/y.txt")
      val storeZ = store.at(s"$workDir/z.txt")
      
      local {
        val failingDep = cmd"cp $nonexistent $storeX"(in = Seq(nonexistent), out = Seq(storeX))
        
        val shouldWork0 = cmd"cp $storeX $storeY"(in = Seq(storeX), out = Seq(storeY))
        val shouldWork1 = cmd"cp $storeX $storeZ"(in = Seq(storeX), out = Seq(storeZ))
        
        ThreeStepsMutualDepFails(scriptContext.projectContext.graph, fails = failingDep, shouldWork0, shouldWork1)
      }
    }
    
    /*
     * As stores:
     *   storeA --> storeB -->  storeC --> storeD
     *                 \
     *                  +--> storeX --> storeY
     *                 /
     *            nonexistent 
     *            
     * As jobs:
     *   
     *   a2b --> b2c --> c2d
     *      \
     *       +- willFail --> x2y           
     */
    val someEarlyFailures: SomeFailures = {
      implicit val scriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
      import LoamPredef._
      import LoamCmdTool._
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store.at(s"$workDir/foo/bar/baz").asInput
      val storeX = store.at(s"$workDir/x.txt")
      val storeY = store.at(s"$workDir/y.txt")
      
      val storeA = store.at("src/test/resources/a.txt").asInput
      val storeB = store.at(s"$workDir/b.txt")
      val storeC = store.at(s"$workDir/c.txt")      
      val storeD = store.at(s"$workDir/d.txt")
      
      local {
        val aToB = cmd"cp $storeA $storeB"(in = Seq(storeA), out = Seq(storeB))
        val fails = cmd"cp $nonexistent $storeX && cp $storeB $storeX"(in = Seq(nonexistent, storeB), out = Seq(storeX))
        val xToY = cmd"cp $storeX $storeY"(in = Seq(storeX), out = Seq(storeY))
        val bToC = cmd"cp $storeB $storeC"(in = Seq(storeB), out = Seq(storeC))
        val cToD = cmd"cp $storeC $storeD"(in = Seq(storeC), out = Seq(storeD))
        val graph = scriptContext.projectContext.graph
        
        SomeFailures(graph, fails, xToY, aToB, bToC, cToD)
      }
    }
  }
}
