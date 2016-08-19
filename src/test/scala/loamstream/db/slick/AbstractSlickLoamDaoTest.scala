package loamstream.db.slick

import scala.util.Try

/**
 * @author clint
 * date: Aug 12, 2016
 */
trait AbstractSlickLoamDaoTest {
  def descriptor: DbDescriptor
  
  protected lazy val dao = new SlickLoamDao(descriptor)
  
  protected def createTablesAndThen[A](f: => A): A = {
    def drop(): Unit = dao.tables.drop(dao.db)
    
    def create(): Unit = dao.tables.create(dao.db)

    //NB: Use Try(...) to succinctly ignore failures
    Try(drop()) 
      
    create()
      
    f
  }
}