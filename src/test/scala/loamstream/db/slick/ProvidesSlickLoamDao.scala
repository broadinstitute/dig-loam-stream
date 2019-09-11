package loamstream.db.slick

import java.nio.file.Path
import java.time.Instant

import scala.util.Try

import loamstream.model.jobs.Execution
import loamstream.model.jobs.StoreRecord
import loamstream.util.Hash
import loamstream.util.Paths
import scala.concurrent.Await
import loamstream.TestHelpers
import org.scalactic.Equality

/**
 * @author clint
 * date: Aug 12, 2016
 */
trait ProvidesSlickLoamDao {
  protected implicit object ExecutionEqualityWithoutSettings extends Equality[Execution] {
    override def areEqual(lhs: Execution, b: Any): Boolean = b match {
      case rhs: Execution => equalityFields(lhs) == equalityFields(rhs)
      case _ => false
    }
    
    private def equalityFields(e: Execution): Seq[_] = Seq(
      e.cmd,
      e.envType,
      e.status,
      e.result,
      e.resources,
      e.outputs,
      e.jobDir,
      e.terminationReason)
  }
  
  protected val descriptor: DbDescriptor = DbDescriptor.inMemory
  
  protected lazy val dao: SlickLoamDao = new SlickLoamDao(descriptor)
  
  protected def store(execution: Execution): Unit = dao.insertExecutions(Seq(execution))

  protected def cachedOutput(path: Path, hash: Hash, lastModified: Instant): StoreRecord = {
    val hashValue = hash.valueAsBase64String

    StoreRecord(
        Paths.normalize(path), 
        () => Option(hashValue), 
        () => Option(hash.tpe.algorithmName), 
        Option(lastModified))
  }

  protected def cachedOutput(path: Path, hash: Hash): StoreRecord = {
    cachedOutput(path, hash, Instant.ofEpochMilli(0))
  }

  protected def failedOutput(path: Path): StoreRecord = StoreRecord(Paths.normalize(path))

  protected def createTablesAndThen[A](f: => A): A = {
    //NB: Use Try(...) to succinctly ignore failures
    Try(dao.dropTables()) 
      
    dao.createTables()
      
    f
  }
  
  protected def executions: Seq[Execution] = {
    import dao.driver.api._
    
    val query = dao.tables.executions.result
  
    val executionsFuture = dao.db.run(query.transactionally)
    
    val executions = TestHelpers.waitFor(executionsFuture)
  
    executions.map(dao.reify)
  }
}
