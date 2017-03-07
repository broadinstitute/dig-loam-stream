package loamstream.db.slick

import java.sql.Timestamp

import loamstream.util.Futures
import loamstream.util.Loggable

import slick.driver.JdbcProfile
import slick.jdbc.meta.MTable

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 * 
 * Class containing objects representing DB tables allowing Slick to be used in 'lifted-embedding' mode.
 * 
 * Tables:
 * 
 *   EXECUTIONS:
 *    ID: integer, auto-incremented, primary key
 *    EXIT_STATUS: integer - the exit status returned by the command represented by this EXECUTION
 *
 *   SETTINGS:
 *    MEM: nullable, float - memory consumption by the job
 *    CPU: nullable, float - cpu usage by the job
 *    START_TIME: nullable, timestamp - when a job started being executed
 *    END_TIME: nullable, timestamp - when a job finished being executed
 *    NODE: nullable, varchar/text - name of the host that has run the job
 *    QUEUE: nullable, varchar/text - name of the cluster queue in which the job has run
 *    EXECUTION_ID: integer, the id of the EXECUTION a row belongs to
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 *   OUTPUTS:
 *     LOCATOR: varchar/text, primary key - the fully-qualified locator of an output
 *     LAST_MODIFIED: nullable, datetime or similar - the last modified time of the file/dir at PATH
 *     HASH: nullable, varchar/text - the hex-coded hash of the file/dir at PATH
 *     HASH_TYPE: nullable, varchar/text - the type of hash performed on PATH; 
 *       see loamstream.util.HashType.fromAlgorithmName
 *     EXECUTION_ID: integer - the id of the EXECUTION a row belongs to
 *     EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *     
 *   The Outputs of failed Executions aren't hashed; in those cases, 
 *   OUTPUT.{LAST_MODIFIED,HASH,HASH_TYPE} will be NULL.
 */
final class Tables(val driver: JdbcProfile) extends Loggable {
  import driver.api._
  import driver.SchemaDescription
  import Tables.Names
  import ForeignKeyAction.{Restrict, Cascade}

  final class Executions(tag: Tag) extends Table[ExecutionRow](tag, Names.executions) {
    def id = column[Int]("ID", O.AutoInc, O.PrimaryKey)
    def exitStatus = column[Int]("EXIT_STATUS")
    def * = (id, exitStatus) <> (ExecutionRow.tupled, ExecutionRow.unapply)
  }

  final class Settings(tag: Tag) extends Table[SettingRow](tag, Names.settings) {
    def executionId = column[Int]("EXECUTION_ID", O.PrimaryKey)
    def mem = column[Option[Float]]("MEM")
    def cpu = column[Option[Float]]("CPU")
    def startTime = column[Option[Timestamp]]("START_TIME")
    def endTime = column[Option[Timestamp]]("END_TIME")
    def node = column[Option[String]]("END_TIME")
    def queue = column[Option[String]]("END_TIME")
    def execution = foreignKey("EXECUTION_FK", executionId, executions)(_.id, onUpdate=Restrict, onDelete=Cascade)
    def * = (executionId, mem, cpu, startTime, endTime, node, queue) <> (SettingRow.tupled, SettingRow.unapply)
  }

  final class Outputs(tag: Tag) extends Table[OutputRow](tag, Names.outputs) {
    def locator = column[String]("LOCATOR", O.PrimaryKey)
    def lastModified = column[Option[Timestamp]]("LAST_MODIFIED")
    def hash = column[Option[String]]("HASH")
    def hashType = column[Option[String]]("HASH_TYPE")
    def executionId = column[Int]("EXECUTION_ID")
    def execution = foreignKey("EXECUTION_FK", executionId, executions)(_.id, onUpdate=Restrict, onDelete=Cascade)
    def * = (locator, lastModified, hash, hashType, executionId.?) <> (OutputRow.tupled, OutputRow.unapply)
  }

  lazy val executions = TableQuery[Executions]
  lazy val settings = TableQuery[Settings]
  lazy val outputs = TableQuery[Outputs]

  private lazy val allTables: Map[String, SchemaDescription] = Map(
    Names.executions -> executions.schema,
    Names.settings -> settings.schema,
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
    val executions = "EXECUTIONS"
    val settings = "SETTINGS"
    val outputs = "OUTPUTS"
  }
}
