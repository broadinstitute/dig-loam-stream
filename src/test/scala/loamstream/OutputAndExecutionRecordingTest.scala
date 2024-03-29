package loamstream

import java.nio.file.Files
import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.loam.LoamCmdTool
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.HashingStrategy
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.model.jobs.StoreRecord
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.Execution
import loamstream.model.execute.DbBackedExecutionRecorder
import loamstream.util.Paths
import loamstream.model.execute.Run
import loamstream.conf.LsSettings


/**
 * @author clint
 * Dec 19, 2017
 */
final class OutputAndExecutionRecordingTest extends FunSuite with ProvidesSlickLoamDao {
  private val run: Run = Run.create()
  
  test("Data about outputs and job executions should be recorded properly") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    import Paths.Implicits._
    
    val out0Path = workDir / "A.copy"
    val out1Path = workDir / "A.copy.copy"
    val out2Path = workDir / "A.copy.copy.copy"
    
    val graph = TestHelpers.makeGraph { implicit context =>
      import loamstream.loam.LoamSyntax.{ store => makeStore, _ }
      
      val in = makeStore("src/test/resources/a.txt").asInput
      val out0 = makeStore(out0Path)
      val out1 = makeStore(out1Path)
      val out2 = makeStore(out2Path)

      local {
        cmd"(echo copying locally 0 ; cp $in $out0)".in(in).out(out0).tag("Local-copy0")

        cmd"(echo copying locally 1 ; cp $out0 $out1)".in(out0).out(out1).tag("Local-copy1")
        
        cmd"(echo copying locally 2 ; cp $out1 $out2)".in(out1).out(out2).tag("Local-copy2")
      }
    }
    
    import Files.exists
    
    registerRunAndThen(run) {
      val jobFilter = new DbBackedJobFilter(dao, HashingStrategy.HashOutputs)
      val executionRecorder = new DbBackedExecutionRecorder(dao, HashingStrategy.HashOutputs)
      
      val executer = RxExecuter.defaultWith(jobFilter, executionRecorder)
      
      val loamEngine = LoamEngine(TestHelpers.config, LsSettings.noCliConfig, LoamCompiler.default, executer)
      
      def out0ExFromDb = findExecution(StoreRecord(out0Path))
      def out1ExFromDb = findExecution(StoreRecord(out1Path))
      def out2ExFromDb = findExecution(StoreRecord(out2Path))
      
      assert(out0ExFromDb.isEmpty)
      assert(out1ExFromDb.isEmpty)
      assert(out2ExFromDb.isEmpty)
      
      assert(!exists(out0Path))
      assert(!exists(out1Path))
      assert(!exists(out2Path))
      
      val results = loamEngine.run(graph)
      
      assert(exists(out0Path))
      assert(exists(out1Path))
      assert(exists(out2Path))
      
      assert(out0ExFromDb.isDefined)
      assert(out1ExFromDb.isDefined)
      assert(out2ExFromDb.isDefined)
      
      def outputField[A](field: StoreRecord => A)(e: Execution.Persisted): A = field(e.outputs.head)
      
      assert(out0ExFromDb.map(outputField(_.isPresent)) === Some(true))
      assert(out0ExFromDb.flatMap(outputField(_.lastModified)).isDefined)
      assert(out0ExFromDb.flatMap(outputField(_.hashType)).isDefined)
      assert(out0ExFromDb.flatMap(outputField(_.hash)).isDefined)
      
      assert(out1ExFromDb.map(outputField(_.isPresent)) === Some(true))
      assert(out1ExFromDb.flatMap(outputField(_.lastModified)).isDefined)
      assert(out1ExFromDb.flatMap(outputField(_.hashType)).isDefined)
      assert(out1ExFromDb.flatMap(outputField(_.hash)).isDefined)
      
      assert(out2ExFromDb.map(outputField(_.isPresent)) === Some(true))
      assert(out2ExFromDb.flatMap(outputField(_.lastModified)).isDefined)
      assert(out2ExFromDb.flatMap(outputField(_.hashType)).isDefined)
      assert(out2ExFromDb.flatMap(outputField(_.hash)).isDefined)
      
      def findJobCopyingTo(p: Path): (LJob, Execution) = {
        val lookingFor = Paths.normalize(p)
        
        results.find { 
          case (j, _) => j.asInstanceOf[CommandLineJob].commandLineString.endsWith(s"${lookingFor})") 
        }.get
      }
      
      val (inTo0, inTo0Execution) = findJobCopyingTo(out0Path)
      val (zeroTo1, zeroTo1Execution) = findJobCopyingTo(out1Path)
      val (oneTo2, oneTo2Execution) = findJobCopyingTo(out2Path)
      
      assert(inTo0Execution.outputs.size === 1)
      assert(inTo0Execution.outputs.head.isPresent)
      assert(inTo0Execution.outputs.head.lastModified.isDefined)
      assert(inTo0Execution.outputs.head.hash.isDefined)
      assert(inTo0Execution.outputs.head.hashType.isDefined)
      
      assert(zeroTo1Execution.outputs.size === 1)
      assert(zeroTo1Execution.outputs.head.isPresent)
      assert(zeroTo1Execution.outputs.head.lastModified.isDefined)
      assert(zeroTo1Execution.outputs.head.hash.isDefined)
      assert(zeroTo1Execution.outputs.head.hashType.isDefined)
      
      assert(oneTo2Execution.outputs.size === 1)
      assert(oneTo2Execution.outputs.head.isPresent)
      assert(oneTo2Execution.outputs.head.lastModified.isDefined)
      assert(oneTo2Execution.outputs.head.hash.isDefined)
      assert(oneTo2Execution.outputs.head.hashType.isDefined)
    }
  }
}
