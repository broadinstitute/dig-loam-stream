package loamstream.db.slick

import slick.driver.JdbcProfile
import java.sql.Timestamp
import slick.jdbc.meta.MTable
import loamstream.util.Futures

/**
 * @author clint
 * date: Sep 22, 2016
 */
final class Tables(val driver: JdbcProfile) {
  import driver.api._
  import driver.SchemaDescription
  import Tables.Names
  import ForeignKeyAction.{Restrict, Cascade}
  
  final class Outputs(tag: Tag) extends Table[RawOutputRow](tag, Names.outputs) {
    def path = column[String]("PATH", O.PrimaryKey)
    def lastModified = column[Timestamp]("LAST_MODIFIED")
    def hash = column[String]("HASH")
    def hashType = column[String]("HASH_TYPE")
    def executionId = column[Int]("EXECUTION_ID")
    def execution = foreignKey("EXECUTION_FK", executionId, executions)(_.id, onUpdate=Restrict, onDelete=Cascade)
    def * = (path, lastModified, hash, hashType, executionId.?) <> (RawOutputRow.tupled, RawOutputRow.unapply) //scalastyle:ignore
  }
  
  final class Executions(tag: Tag) extends Table[RawExecutionRow](tag, Names.executions) {
    def id = column[Int]("ID", O.AutoInc, O.PrimaryKey)
    def exitStatus = column[Int]("EXIT_STATUS")
    def * = (id.?, exitStatus) <> (RawExecutionRow.tupled, RawExecutionRow.unapply)
  }
  
  lazy val outputs = TableQuery[Outputs]
  
  lazy val executions = TableQuery[Executions]

  private lazy val allTables: Map[String, SchemaDescription] = Map(
    Names.outputs -> outputs.schema, 
    Names.executions -> executions.schema
  )
  
  //private def ddlForAllTables = outputs.schema ++ jobs.schema
  private def ddlForAllTables = allTables.values.reduce(_ ++ _)
  
  def create(database: Database): Unit = {
    def exists(tableName: String): Boolean = {
      val existingTables = perform(database)(MTable.getTables)
      
      existingTables.exists(_.name == tableName)
    }
    
    for {
      (tableName, schema) <- allTables
      if !exists(tableName)
    } yield {
      //TODO: Find a way to compose all these create actions
      perform(database)(schema.create.transactionally)
    }
  }
  
  def drop(database: Database): Unit = perform(database)(ddlForAllTables.drop.transactionally)

  private def perform[A](database: Database)(action: DBIO[A]): A = {
    Futures.waitFor(database.run(action))
  }
}

object Tables {
  object Names {
    val outputs = "OUTPUTS"
    val executions = "EXECUTIONS"
  }
}