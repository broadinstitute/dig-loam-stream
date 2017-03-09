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
 *    ENV: varchar/text - the platform where this execution took place (e.g. Uger, Google, Local)
 *    EXIT_STATUS: integer - the exit status returned by the command represented by this execution
 *
 *
 *   OUTPUTS: The Outputs of failed Executions aren't hashed; in those cases,
 *            OUTPUT.{LAST_MODIFIED,HASH,HASH_TYPE} will be NULL.
 *    LOCATOR: varchar/text, primary key - the fully-qualified locator of an output
 *    LAST_MODIFIED: timestamp, nullable - the last modified time of the file/dir at PATH
 *    HASH: varchar/text, nullable - the hex-coded hash of the file/dir at PATH
 *    HASH_TYPE: varchar/text, nullable - the type of hash performed on PATH;
 *                                        see loamstream.util.HashType.fromAlgorithmName
 *    EXECUTION_ID: integer, primary key - ID of the EXECUTION a row belongs to
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 *
 *   SETTINGS_UGER: Environment settings requested during Uger job submissions
 *    EXECUTION_ID: integer, primary key - ID of the execution a row belongs to
 *    MEM: integer - memory requested when submitting the job
 *    CPU: integer - number of cpu's requested when submitting the job
 *    QUEUE: varchar/text - name of the cluster queue in which the job has run
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 *
 *   SETTINGS_GOOGLE: Environment settings requested during Google Cloud job submissions
 *    EXECUTION_ID: integer, primary key - ID of the execution a row belongs to
 *    MEM: integer - memory requested when submitting the job
 *    CPU: integer - number of cpu's requested when submitting the job
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 *
 *   SETTINGS_LOCAL: Environment settings requested during local job submissions
 *    EXECUTION_ID: integer, primary key - ID of the execution a row belongs to
 *    MEM: integer - memory requested when submitting the job
 *    CPU: integer - number of cpu's requested when submitting the job
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 *
 *   RESOURCES: Resources used during the job's execution
 *    EXECUTION_ID: integer, primary key - ID of the execution a row belongs to
 *    MEM: float, nullable - memory requested when submitting the job
 *    CPU: float, nullable - number of cpu's requested when submitting the job
 *    START_TIME: timestamp, nullable - when a job started being executed
 *    END_TIME: timestamp, nullable - when a job finished being executed
 *    NODE: varchar/text, nullable - name of the host that has run the job
 *    QUEUE: varchar/text, nullable - name of the cluster queue in which the job has run
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 */
final class Tables(val driver: JdbcProfile) extends Loggable {
  import driver.api._
  import driver.SchemaDescription
  import Tables.Names
  import ForeignKeyAction.{Restrict, Cascade}

  final class Executions(tag: Tag) extends Table[ExecutionRow](tag, Names.executions) {
    def id = column[Int]("ID", O.AutoInc, O.PrimaryKey)
    def env = column[String]("ENV")
    def exitStatus = column[Int]("EXIT_STATUS")
    def * = (id, env, exitStatus) <> (ExecutionRow.tupled, ExecutionRow.unapply)
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

  final class UgerSettings(tag: Tag) extends Table[UgerSettingRow](tag, Names.ugerSettings) {
    def executionId = column[Int]("EXECUTION_ID", O.PrimaryKey)
    def mem = column[Int]("MEM")
    def cpu = column[Int]("CPU")
    def queue = column[String]("QUEUE")
    def execution = foreignKey("EXECUTION_FK", executionId, executions)(_.id, onUpdate=Restrict, onDelete=Cascade)
    def * = (executionId, mem, cpu, queue) <> (UgerSettingRow.tupled, UgerSettingRow.unapply)
  }

  lazy val executions = TableQuery[Executions]
  lazy val outputs = TableQuery[Outputs]
  lazy val ugerSettings = TableQuery[UgerSettings]

  private lazy val allTables: Map[String, SchemaDescription] = Map(
    Names.executions -> executions.schema,
    Names.outputs -> outputs.schema,
    Names.ugerSettings -> ugerSettings.schema
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
    val outputs = "OUTPUTS"
    val ugerSettings = "SETTINGS_UGER"
  }
}
