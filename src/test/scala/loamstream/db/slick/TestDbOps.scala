package loamstream.db.slick

import scala.util.Try

import loamstream.TestHelpers
import loamstream.model.jobs.Execution
import loamstream.model.jobs.StoreRecord
import loamstream.model.jobs.JobResult.CommandResult
import java.nio.file.Paths
import loamstream.model.execute.EnvironmentType
import loamstream.model.jobs.TerminationReason
import loamstream.model.jobs.PseudoExecution

/**
 * @author clint
 * Sep 12, 2019
 */
trait TestDbOps {
  protected val dao: SlickLoamDao
  
  protected def store(execution: Execution): Unit = dao.insertExecutions(Seq(execution))
  
  protected def executions: Seq[Execution.Persisted] = {
    import dao.driver.api._
    
    val query = dao.tables.executions.result
  
    val executionsFuture = dao.db.run(query.transactionally)
    
    val executions = TestHelpers.waitFor(executionsFuture)
  
    executions.map(reify)
  }
  
  private def reify(executionRow: ExecutionRow): Execution.Persisted = {
    import executionRow._
    
    val commandResult = CommandResult(exitCode)

    import Paths.{get => toPath}
    
    val termReason = terminationReason.flatMap(TerminationReason.fromName)
    
    val envTypeOpt = EnvironmentType.fromString(env)
    
    require(envTypeOpt.isDefined, s"Unknown environment type name '${env}'")
    
    PseudoExecution(
        envType = envTypeOpt.get,
        cmd = cmd,
        status = status,
        result = Option(commandResult),
        outputs = dao.outputsFor(executionRow).toSet,
        jobDir = jobDir.map(toPath(_)),
        terminationReason = termReason)
  }
  
  protected def findExecution(output: StoreRecord): Option[Execution.Persisted] = findExecution(output.loc)
  
  protected def findExecution(outputLocation: String): Option[Execution.Persisted] = {
    import dao.driver.api._
    
    val executionForPath = for {
      output <- dao.tables.outputs.filter(_.locator === outputLocation)
      execution <- output.execution
    } yield {
      execution
    }

    val query = executionForPath.result.headOption

    val executionOptFuture = dao.db.run(query.transactionally)
    
    val executionOpt = TestHelpers.waitFor(executionOptFuture)
    
    executionOpt.map(reify)
  }
  
  protected def createTablesAndThen[A](f: => A): A = {
    //NB: Use Try(...) to succinctly ignore failures
    Try(dao.dropTables()) 
      
    dao.createTables()
      
    f
  }
}
