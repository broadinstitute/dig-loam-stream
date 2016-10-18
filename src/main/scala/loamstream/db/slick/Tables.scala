package loamstream.db.slick

import java.sql.Timestamp

import loamstream.util.Futures
import loamstream.util.Loggable

import slick.driver.JdbcProfile
import slick.jdbc.meta.MTable

/**
 * @author clint
 * date: Sep 22, 2016
 */
final class Tables(val driver: JdbcProfile) extends Loggable {
  import driver.api._
  import driver.SchemaDescription
  import Tables.Names
  import ForeignKeyAction.{Restrict, Cascade}
  
  final class Outputs(tag: Tag) extends Table[OutputRow](tag, Names.outputs) {
    def path = column[String]("PATH", O.PrimaryKey)
    def lastModified = column[Option[Timestamp]]("LAST_MODIFIED")
    def hash = column[Option[String]]("HASH")
    def hashType = column[Option[String]]("HASH_TYPE")
    def executionId = column[Int]("EXECUTION_ID")
    def execution = foreignKey("EXECUTION_FK", executionId, executions)(_.id, onUpdate=Restrict, onDelete=Cascade)
    def * = (path, lastModified, hash, hashType, executionId.?) <> (OutputRow.tupled, OutputRow.unapply)
  }
  
  final class Executions(tag: Tag) extends Table[ExecutionRow](tag, Names.executions) {
    def id = column[Int]("ID", O.AutoInc, O.PrimaryKey)
    def exitStatus = column[Int]("EXIT_STATUS")
    def * = (id, exitStatus) <> (ExecutionRow.tupled, ExecutionRow.unapply)
  }
  
  lazy val executions = TableQuery[Executions]
  
  lazy val outputs = TableQuery[Outputs]
  
  private lazy val allTables: Map[String, SchemaDescription] = Map(
    Names.executions -> executions.schema,
    Names.outputs -> outputs.schema
  )
  
  private def allTableNames: Seq[String] = allTables.keys.toSeq
  
  private def allSchemas: Seq[SchemaDescription] = allTables.values.toSeq
  
  private def ddlForAllTables = allSchemas.reduce(_ ++ _)
  
  def create(database: Database): Unit = {
    //TODO: Is this appropriate?
    implicit val executionContext = database.executor.executionContext
    
    val existingTableNames = for {
      tables <- MTable.getTables
    } yield tables.map(_.name.name).toSet
    
    val existing = perform(database)(existingTableNames)
    
    val createActions = for {
      (tableName, schema) <- allTables
      if !existing.contains(tableName)
    } yield {
      log(schema)
      
      schema.create
    }
    
    val createEverythingAction = DBIO.sequence(createActions).transactionally
    
    perform(database)(createEverythingAction)
  }
  
  def drop(database: Database): Unit = perform(database)(ddlForAllTables.drop.transactionally)

  private def log(schema: SchemaDescription): Unit = {
    schema.createStatements.foreach(s => debug(s"DDL: $s"))
  }
  
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