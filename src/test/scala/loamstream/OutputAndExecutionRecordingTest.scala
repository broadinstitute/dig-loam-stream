package loamstream

import org.scalatest.FunSuite
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool
import loamstream.util.PathEnrichments
import loamstream.compiler.LoamEngine
import loamstream.model.execute.RxExecuter
import loamstream.apps.AppWiring
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.HashingStrategy
import loamstream.compiler.LoamCompiler
import loamstream.model.jobs.OutputRecord
import java.nio.file.Files
import loamstream.db.slick.ProvidesSlickLoamDao
import java.nio.file.Path
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.PathUtils
import loamstream.model.jobs.Execution

/**
 * @author clint
 * Dec 19, 2017
 */
final class OutputAndExecutionRecordingTest extends FunSuite with ProvidesSlickLoamDao {
  test("Data about outputs and job executions should be recorded properly") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    import PathEnrichments._
    
    val out0Path = workDir / "A.copy"
    val out1Path = workDir / "A.copy.copy"
    val out2Path = workDir / "A.copy.copy.copy"
    
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef.{ store => str, _ }
      import LoamCmdTool._
      
      val in = str.at("src/test/resources/a.txt").asInput
      val out0 = str.at(out0Path)
      val out1 = str.at(out1Path)
      val out2 = str.at(out2Path)

      local {
        cmd"(echo copying locally 0 ; cp $in $out0)".in(in).out(out0).named("Local-copy0")

        cmd"(echo copying locally 1 ; cp $out0 $out1)".in(out0).out(out1).named("Local-copy1")
        
        cmd"(echo copying locally 2 ; cp $out1 $out2)".in(out1).out(out2).named("Local-copy2")
      }
    }
    
    import Files.exists
    
    createTablesAndThen {
      val jobFilter = new DbBackedJobFilter(dao, HashingStrategy.HashOutputs)
      
      val executer = RxExecuter.defaultWith(jobFilter)
      
      val loamEngine = LoamEngine(TestHelpers.config, LoamCompiler.default, executer, jobFilter)
      
      def out0ExFromDb = dao.findExecution(OutputRecord(out0Path))
      def out1ExFromDb = dao.findExecution(OutputRecord(out1Path))
      def out2ExFromDb = dao.findExecution(OutputRecord(out2Path))
      
      assert(out0ExFromDb.isEmpty)
      assert(out1ExFromDb.isEmpty)
      assert(out2ExFromDb.isEmpty)
      
      assert(!exists(out0Path))
      assert(!exists(out1Path))
      assert(!exists(out2Path))
      
      val results = loamEngine.run(graph)
      
      assert(out0ExFromDb.isDefined)
      assert(out1ExFromDb.isDefined)
      assert(out2ExFromDb.isDefined)
      
      assert(exists(out0Path))
      assert(exists(out1Path))
      assert(exists(out2Path))
      
      def outputField[A](field: OutputRecord => A)(e: Execution): A = field(e.outputs.head)
      
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
      
      import PathUtils.normalize
      
      def findJobCopyingTo(p: Path): (LJob, Execution) = {
        val lookingFor = normalize(p)
        
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
