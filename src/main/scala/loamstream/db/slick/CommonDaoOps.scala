package loamstream.db.slick

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import loamstream.util.Loggable
import slick.jdbc.JdbcProfile

/**
 * @author clint
 * Dec 7, 2017
 * 
 * NB: Factored out of SlickLoamDao, which had gotten huge
 */
trait CommonDaoOps extends DbHelpers with Loggable {
  def descriptor: DbDescriptor
  
  override val driver: JdbcProfile = descriptor.dbType.driver
  
  import driver.api._
  
  type WriteAction[A] =  driver.ProfileAction[A, NoStream, Effect.Write]
  
  protected[slick] lazy val db: driver.backend.DatabaseDef = {
    Database.forURL(url = descriptor.url, driver = descriptor.dbType.jdbcDriverClass)
  }

  protected[slick] lazy val tables: Tables = new Tables(driver)

  def createTables(): Unit = tables.create(db)

  def dropTables(): Unit = tables.drop(db)

  def shutdown(): Unit = blockOn(db.shutdown)
  
  protected[slick] def runBlocking[A](action: DBIO[A]): A = runBlocking(db)(action)
  
  protected def log(sqlAction: driver.ProfileAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => trace(s"SQL: $s"))
  }
  
  private[slick] object Implicits {
    //TODO: re-evaluate; does this make sense?
    implicit val dbExecutionContext: ExecutionContext = db.executor.executionContext
  }
}
