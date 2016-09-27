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
    def * = (id, exitStatus) <> (RawExecutionRow.tupled, RawExecutionRow.unapply)
  }
  
  lazy val outputs = TableQuery[Outputs]
  
  lazy val executions = TableQuery[Executions]

  private lazy val allTables: Seq[(String, SchemaDescription)] = Seq(
    Names.executions -> executions.schema,
    Names.outputs -> outputs.schema 
  )
  
  private def allTableNames: Seq[String] = allTables.unzip._1
  
  private def allSchemas: Seq[SchemaDescription] = allTables.unzip._2
  
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
      schema.createStatements.foreach(s => println(s"Tables.create: DDL: $s"))
      
      schema.create
    }
    
    val createEverythingAction = DBIO.sequence(createActions).transactionally
    
    perform(database)(createEverythingAction)
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