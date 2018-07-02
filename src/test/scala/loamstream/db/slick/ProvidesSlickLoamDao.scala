package loamstream.db.slick

import java.nio.file.Path
import java.time.Instant

import scala.util.Try

import loamstream.model.jobs.Execution
import loamstream.model.jobs.OutputRecord
import loamstream.util.Hash
import loamstream.util.PathUtils

/**
 * @author clint
 * date: Aug 12, 2016
 */
trait ProvidesSlickLoamDao {
  protected val descriptor: DbDescriptor = DbDescriptor.inMemory
  
  protected lazy val dao: SlickLoamDao = new SlickLoamDao(descriptor)
  
  protected def store(execution: Execution): Unit = dao.insertExecutions(Seq(execution))

  protected def cachedOutput(path: Path, hash: Hash, lastModified: Instant): OutputRecord = {
    val hashValue = hash.valueAsBase64String

    OutputRecord(PathUtils.normalize(path), Option(hashValue), Option(hash.tpe.algorithmName), Option(lastModified))
  }

  protected def cachedOutput(path: Path, hash: Hash): OutputRecord = {
    cachedOutput(path, hash, Instant.ofEpochMilli(0))
  }

  protected def failedOutput(path: Path): OutputRecord = OutputRecord(PathUtils.normalize(path))

  protected def createTablesAndThen[A](f: => A): A = {
    //NB: Use Try(...) to succinctly ignore failures
    Try(dao.dropTables()) 
      
    dao.createTables()
      
    f
  }
  
  protected def executions: Set[Execution] = dao.allExecutions.toSet
}
