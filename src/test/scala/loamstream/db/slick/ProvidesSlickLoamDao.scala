package loamstream.db.slick

import scala.util.Try

import loamstream.model.jobs.Execution

/**
 * @author clint
 * date: Aug 12, 2016
 */
trait ProvidesSlickLoamDao {
  protected val descriptor: DbDescriptor = TestDbDescriptors.inMemoryH2
  
  protected lazy val dao = new SlickLoamDao(descriptor)
  
  protected def store(execution: Execution): Unit = dao.insertExecutions(Seq(execution))
  
  protected def createTablesAndThen[A](f: => A): A = {
    //NB: Use Try(...) to succinctly ignore failures
    Try(dao.dropTables()) 
      
    dao.createTables()
      
    f
  }
}
