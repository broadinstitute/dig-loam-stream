package loamstream.db.slick

import scala.util.Try

/**
 * @author clint
 * date: Aug 12, 2016
 */
trait ProvidesSlickLoamDao {
  def descriptor: DbDescriptor
  
  protected lazy val dao = new SlickLoamDao(descriptor)
  
  protected def createTablesAndThen[A](f: => A): A = {
    //NB: Use Try(...) to succinctly ignore failures
    Try(dao.dropTables()) 
      
    dao.createTables()
      
    f
  }
}