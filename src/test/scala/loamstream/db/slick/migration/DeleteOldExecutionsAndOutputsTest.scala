package loamstream.db.slick.migration

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.TestHelpers.path
import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.execute.RxExecuter
import loamstream.loam.LoamSyntax
import loamstream.loam.LoamGraph
import loamstream.compiler.LoamEngine
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.execute.DbBackedExecutionRecorder
import loamstream.db.slick.SlickLoamDao
import scala.util.Success
import loamstream.model.execute.Executable

/**
 * @author clint
 * Apr 22, 2020
 */
final class DeleteOldExecutionsAndOutputsTest extends FunSuite with ProvidesSlickLoamDao {
  test("migrate") {
    createTablesAndThen {
      TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
        val executionConfig = ExecutionConfig.default.copy(jobDataDir = workDir.resolve("jobs"))
        
        val dbDescriptor = descriptor
        
        val executer = RxExecuter(
            executionConfig = executionConfig, 
            executionRecorder = new DbBackedExecutionRecorder(new SlickLoamDao(dbDescriptor)))

        def run(executable: Executable): Unit = assert(executer.execute(executable).values.forall(_.isSuccess))
            
        def graph(runNumber: Int): LoamGraph = TestHelpers.makeGraph { implicit scriptContext =>
          import LoamSyntax._
          
          val output = LoamSyntax.store(workDir / s"${runNumber}.txt")
            
          cmd"echo ${runNumber} > ${output}".out(output)
        }
        
        def executable(i: Int) = LoamEngine.toExecutable(graph(i))
        
        assert(executions.size === 0)
        assert(outputs.size === 0)
        
        run(executable(1))
        
        assert(executions.size === 1)
        assert(outputs.size === 1)
        
        run(executable(2))
        
        assert(executions.size === 2)
        assert(outputs.size === 2)

        val executable3 = executable(3)
        
        run(executable3)
        
        assert(executions.size === 3)
        assert(outputs.size === 3)
        
        assert((new DeleteOldExecutionsAndOutputs(dbDescriptor, executable3)).migrate() === Success(()))
        
        assert(executions.size === 1)
        assert(outputs.size === 1)
        
        assert(executions.map(_.cmd) === Seq(Some(s"echo 3 > ${workDir}/3.txt")))
        assert(outputs.map(_.loc) === Seq(s"${workDir}/3.txt"))
        
        assert((new DeleteOldExecutionsAndOutputs(dbDescriptor, executable3)).migrate() === Success(()))
        
        assert(executions.size === 1)
        assert(outputs.size === 1)
        
        assert(executions.map(_.cmd) === Seq(Some(s"echo 3 > ${workDir}/3.txt")))
        assert(outputs.map(_.loc) === Seq(s"${workDir}/3.txt"))
        
        val executable4 = executable(4)
        
        run(executable4)
        
        assert(executions.size === 2)
        assert(outputs.size === 2)
        
        assert((new DeleteOldExecutionsAndOutputs(dbDescriptor, executable4)).migrate() === Success(()))
        
        assert(executions.map(_.cmd) === Seq(Some(s"echo 4 > ${workDir}/4.txt")))
        assert(outputs.map(_.loc) === Seq(s"${workDir}/4.txt"))
        
        assert(executions.size === 1)
        assert(outputs.size === 1)
      }
    }
  }
}
