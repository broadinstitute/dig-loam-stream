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
import loamstream.compiler.LoamEngine
import loamstream.model.jobs.Execution
import scala.concurrent.duration.Duration
import loamstream.model.jobs.NoOpJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult

/**
 * @author clint
 * Sep 14, 2017
 */
final class LoamstreamShouldntHangTest extends FunSuite {
  
  private def getJobFor(results: Map[LJob, Execution])(tool: LoamCmdTool): LJob = {
    def notNoOp(j: LJob): Boolean = !j.isInstanceOf[NoOpJob]
    def asCLSJ(j: LJob): CommandLineStringJob = j.asInstanceOf[CommandLineStringJob]
    
    results.keys.filter(notNoOp).map(asCLSJ).find(_.commandLineString == tool.commandLine).get
  }
  
  private def doTest(descriptor: Pipelines.Descriptor, timeout: Duration = Duration.Inf): Unit = {
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
      import StoreType.TXT
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store[TXT].at(s"$workDir/foo/bar/baz").asInput
      val storeX = store[TXT].at(s"$workDir/x.txt")
      
      local {
        val fails = cmd"cp $nonexistent $storeX".in(nonexistent).out(storeX)
        
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
      import StoreType.TXT
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store[TXT].at(s"$workDir/foo/bar/baz").asInput
      val storeX = store[TXT].at(s"$workDir/x.txt")
      val storeY = store[TXT].at(s"$workDir/y.txt")
      
      local {
        val fails = cmd"cp $nonexistent $storeX".in(nonexistent).out(storeX)
        val shouldWork = cmd"cp $storeX $storeY".in(storeX).out(storeY)
        
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
      import StoreType.TXT
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val storeA = store[TXT].at("src/test/resources/a.txt").asInput
      val storeB = store[TXT].at(s"$workDir/b.txt")
      val workDirStore = store[TXT].at(workDir)
      
      local {
        val shouldWork = cmd"cp $storeA $storeB".in(storeA).out(storeB)
        //NB: Fails since cat/bash won't write a file over a directory
        val fails = cmd"cat $storeB > $workDirStore".in(storeB).out(workDirStore)
        
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
      import StoreType.TXT
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store[TXT].at(s"$workDir/asdf/asdf")
      val storeX = store[TXT].at(s"$workDir/x.txt")
      val storeY = store[TXT].at(s"$workDir/y.txt")
      val storeZ = store[TXT].at(s"$workDir/z.txt")
      
      local {
        val failingDep = cmd"cp $nonexistent $storeX".in(nonexistent).out(storeX)
        
        val shouldWork0 = cmd"cp $storeX $storeY".in(storeX).out(storeY)
        val shouldWork1 = cmd"cp $storeX $storeZ".in(storeX).out(storeZ)
        
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
      import StoreType.TXT
      
      val workDir = TestHelpers.getWorkDir(s"${classOf[LoamstreamShouldntHangTest].getSimpleName}")
      
      val nonexistent = store[TXT].at(s"$workDir/foo/bar/baz").asInput
      val storeX = store[TXT].at(s"$workDir/x.txt")
      val storeY = store[TXT].at(s"$workDir/y.txt")
      
      val storeA = store[TXT].at("src/test/resources/a.txt").asInput
      val storeB = store[TXT].at(s"$workDir/b.txt")
      val storeC = store[TXT].at(s"$workDir/c.txt")      
      val storeD = store[TXT].at(s"$workDir/d.txt")
      
      local {
        val aToB = cmd"cp $storeA $storeB".in(storeA).out(storeB)
        val fails = cmd"cp $nonexistent $storeX && cp $storeB $storeX".in(nonexistent, storeB).out(storeX)
        val xToY = cmd"cp $storeX $storeY".in(storeX).out(storeY)
        val bToC = cmd"cp $storeB $storeC".in(storeB).out(storeC)
        val cToD = cmd"cp $storeC $storeD".in(storeC).out(storeD)
        val graph = scriptContext.projectContext.graph
        
        SomeFailures(graph, fails, xToY, aToB, bToC, cToD)
      }
    }
  }
}
