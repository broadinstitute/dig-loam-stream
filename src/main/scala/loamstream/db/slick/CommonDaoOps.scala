package loamstream.db.slick

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import loamstream.util.Loggable
import slick.jdbc.JdbcProfile
import slick.sql.SqlAction
import loamstream.db.LoamDao

/**
 * @author clint
 * Dec 7, 2017
 * 
 * NB: Factored out of SlickLoamDao, which had gotten huge
 */
trait CommonDaoOps extends DbHelpers with Loggable { self: LoamDao =>
  def descriptor: DbDescriptor
  
  override val driver: JdbcProfile = descriptor.dbType.driver
  
  import driver.api._
  
  type WriteAction[A] =  driver.ProfileAction[A, NoStream, Effect.Write]
  
  protected[slick] lazy val db: driver.backend.DatabaseDef = {
    Database.forURL(url = descriptor.url, driver = descriptor.dbType.jdbcDriverClass)
  }

  protected[slick] lazy val tables: Tables = new Tables(driver)

  override def createTables(): Unit = tables.create(db)

  override def dropTables(): Unit = tables.drop(db)

  override def shutdown(): Unit = blockOn(db.shutdown)
  
  protected[slick] def runBlocking[A](action: DBIO[A]): A = runBlocking(db)(action)
  
  protected def log(sqlAction: SqlAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => trace(s"SQL: $s"))
  }
  
  private[slick] object Implicits {
    //TODO: re-evaluate; does this make sense?
    implicit val dbExecutionContext: ExecutionContext = db.executor.executionContext
  }
}
