package loamstream.db.slick

import scala.util.Try

import loamstream.TestHelpers
import loamstream.model.jobs.Execution
import loamstream.model.jobs.StoreRecord

/**
 * @author clint
 * Sep 12, 2019
 */
trait TestDbOps {
  protected val dao: SlickLoamDao
  
  protected def store(execution: Execution): Unit = dao.insertExecutions(Seq(execution))
  
  protected def executions: Seq[Execution] = {
    import dao.driver.api._
    
    val query = dao.tables.executions.result
  
    val executionsFuture = dao.db.run(query.transactionally)
    
    val executions = TestHelpers.waitFor(executionsFuture)
  
    executions.map(dao.reify)
  }
  
  protected def findExecution(outputStoreRecord: StoreRecord): Option[Execution] = {
    findExecution(outputStoreRecord.loc)
  }
  
  protected def findExecution(outputLocation: String): Option[Execution] = {
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
    
    executionOpt.map(dao.reify)
  }
  
  protected def createTablesAndThen[A](f: => A): A = {
    //NB: Use Try(...) to succinctly ignore failures
    Try(dao.dropTables()) 
      
    dao.createTables()
      
    f
  }
}
