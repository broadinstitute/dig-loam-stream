package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.jobs.JobStatus
import loamstream.util.Futures
import loamstream.util.Loggable
import slick.jdbc.JdbcProfile
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
 *    CMD: varchar/text/etc - the command line string -- may be 4000 characters or longer
 *    EXIT_CODE: integer - the exit code returned by the command represented by this execution
 *    STATUS: JobStatus - the state of this execution
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
 *   SETTINGS_LOCAL: Environment settings requested during local job submissions
 *    EXECUTION_ID: integer, primary key - ID of the execution a row belongs to
 *    MEM: integer, nullable - memory requested when submitting the job
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
 *    CLUSTER: varchar/text - name of the cluster where the job has been submitted to
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 *
 *   RESOURCES_LOCAL: Resources used during a local job's execution
 *    EXECUTION_ID: integer, primary key - ID of the execution a row belongs to
 *    START_TIME: timestamp, nullable - when a job started being executed
 *    END_TIME: timestamp, nullable - when a job finished being executed
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 *
 *   RESOURCES_UGER: Resources used during a UGER job's execution
 *    EXECUTION_ID: integer, primary key - ID of the execution a row belongs to
 *    MEM: float, nullable - memory used by the job
 *    CPU: float, nullable - cpu time taken by the job
 *    START_TIME: timestamp, nullable - when a job started being executed
 *    END_TIME: timestamp, nullable - when a job finished being executed
 *    NODE: varchar/text, nullable - name of the host that has run the job
 *    QUEUE: varchar/text, nullable - name of the cluster queue in which the job has run
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 *
 *   RESOURCES_GOOGLE: Resources used during a Google Cloud job's execution
 *    EXECUTION_ID: integer, primary key - ID of the execution a row belongs to
 *    CLUSTER: varchar/text, nullable - name of the cluster that has run the job
 *    START_TIME: timestamp, nullable - when a job started being executed
 *    END_TIME: timestamp, nullable - when a job finished being executed
 *    EXECUTION_FK: a foreign-key constraint from OUTPUTS.EXECUTION_ID to EXECUTION.ID
 *
 */
final class Tables(val driver: JdbcProfile) extends DbHelpers with Loggable {
  import driver.api._
  import driver.SchemaDescription
  import Tables.Names
  import ForeignKeyAction.{Restrict, Cascade}

  private implicit val statusColumnType: BaseColumnType[JobStatus] = {
    MappedColumnType.base[JobStatus, String](_.toString, jobStatusfromString _)
  }

  trait HasExecutionId { self: Table[_] =>
    def executionId: Rep[Int]
  }

  abstract class BelongsToExecution[R](tag: Tag, name: String) extends Table[R](tag, name) with HasExecutionId {
    final val foreignKey = s"$foreignKeyPrefix${name}"
    
    final def execution = foreignKey(foreignKey, executionId, executions)(_.id, onUpdate=Restrict, onDelete=Cascade)
  }
  
  //Use Int.MaxValue as an approximation of maximum String length, since Strings are represented as
  //char arrays indexed by integers on the JVM, and String.length() returns an int.
  private[this] val maxStringColumnLength = Int.MaxValue
  
  final class Executions(tag: Tag) extends Table[ExecutionRow](tag, Names.executions) {
    def id = column[Int]("ID", O.AutoInc, O.PrimaryKey)
    def env = column[String]("ENV")
    //NB: Specify the length of this column so that we hopefully don't get a too-small VARCHAR,
    //and instead some DB-specific column type appropriate for strings thousands of chars long.
    def cmd = column[String]("CMD", O.Length(maxStringColumnLength))
    def stdoutPath = column[String]("STDOUT_PATH", O.Length(maxStringColumnLength))
    def stderrPath = column[String]("STDERR_PATH", O.Length(maxStringColumnLength))
    def exitCode = column[Int]("EXIT_CODE")
    def status = column[JobStatus]("STATUS")
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = {
      (id, env, cmd, status, exitCode, stdoutPath, stderrPath) <> (ExecutionRow.tupled, ExecutionRow.unapply)
    }
  }

  final class Outputs(tag: Tag) extends BelongsToExecution[OutputRow](tag, Names.outputs) {
    def locator = column[String]("LOCATOR", O.PrimaryKey)
    def lastModified = column[Option[Timestamp]]("LAST_MODIFIED")
    def hash = column[Option[String]]("HASH")
    def hashType = column[Option[String]]("HASH_TYPE")
    override def executionId = column[Int]("EXECUTION_ID")
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = (locator, lastModified, hash, hashType, executionId.?) <> (OutputRow.tupled, OutputRow.unapply)
  }

  final class LocalSettings(tag: Tag) extends BelongsToExecution[LocalSettingRow](tag, Names.localSettings) {
    override def executionId = column[Int]("EXECUTION_ID", O.PrimaryKey)
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = (executionId) <> (LocalSettingRow.tupled, LocalSettingRow.unapply)
  }

  private[slick] abstract class DrmSettingsTable[R](
      tag: Tag, 
      name: String) extends BelongsToExecution[R](tag, name) {
    
    override def executionId = column[Int]("EXECUTION_ID", O.PrimaryKey)
    def cpus = column[Int]("CPU")
    def memPerCpu = column[Double]("MEM")
    def maxRunTime = column[Double]("MAX_RUN_TIME")
    def queue = column[Option[String]]("QUEUE")
  }
  
  final class UgerSettings(tag: Tag) extends DrmSettingsTable[UgerSettingRow](tag, Names.ugerSettings) {
    override def * = {
      (executionId, cpus, memPerCpu, maxRunTime, queue) <> (UgerSettingRow.tupled, UgerSettingRow.unapply)
    }
  }
  
  final class LsfSettings(tag: Tag) extends DrmSettingsTable[LsfSettingRow](tag, Names.lsfSettings) {
    override def * = (executionId, cpus, memPerCpu, maxRunTime, queue) <> (LsfSettingRow.tupled, LsfSettingRow.unapply)
  }

  final class GoogleSettings(tag: Tag) extends BelongsToExecution[GoogleSettingRow](tag, Names.googleSettings) {
    override def executionId = column[Int]("EXECUTION_ID", O.PrimaryKey)
    def cluster = column[String]("CLUSTER")
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = (executionId, cluster) <> (GoogleSettingRow.tupled, GoogleSettingRow.unapply)
  }

  final class LocalResources(tag: Tag) extends BelongsToExecution[LocalResourceRow](tag, Names.localResources) {
    override def executionId = column[Int]("EXECUTION_ID", O.PrimaryKey)
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Timestamp]("END_TIME")
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = (executionId, startTime, endTime) <> (LocalResourceRow.tupled, LocalResourceRow.unapply)
  }

  private[slick] abstract class DrmResourcesTable[R](tag: Tag, name: String) extends BelongsToExecution[R](tag, name) {
    
    override def executionId = column[Int]("EXECUTION_ID", O.PrimaryKey)
    def mem = column[Double]("MEM")
    def cpu = column[Double]("CPU")
    def node = column[Option[String]]("NODE")
    def queue = column[Option[String]]("QUEUE")
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Timestamp]("END_TIME")
  }
  
  final class UgerResources(tag: Tag) extends DrmResourcesTable[UgerResourceRow](tag, Names.ugerResources) {
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = {
      (executionId, mem, cpu, node, queue, startTime, endTime) <> (UgerResourceRow.tupled, UgerResourceRow.unapply)
    }
  }
  
  final class LsfResources(tag: Tag) extends DrmResourcesTable[LsfResourceRow](tag, Names.lsfResources) {
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = {
      (executionId, mem, cpu, node, queue, startTime, endTime) <> (LsfResourceRow.tupled, LsfResourceRow.unapply)
    }
  }

  final class GoogleResources(tag: Tag) extends BelongsToExecution[GoogleResourceRow](tag, Names.googleResources) {
    
    override def executionId = column[Int]("EXECUTION_ID", O.PrimaryKey)
    def cluster = column[String]("CLUSTER")
    def startTime = column[Timestamp]("START_TIME")
    def endTime = column[Timestamp]("END_TIME")
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = {
      (executionId, cluster, startTime, endTime) <> (GoogleResourceRow.tupled, GoogleResourceRow.unapply)
    }
  }

  private val foreignKeyPrefix = s"FK_ID_EXECUTIONS_"

  lazy val executions = TableQuery[Executions]
  lazy val outputs = TableQuery[Outputs]
  lazy val localSettings = TableQuery[LocalSettings]
  lazy val ugerSettings = TableQuery[UgerSettings]
  lazy val lsfSettings = TableQuery[LsfSettings]
  lazy val googleSettings = TableQuery[GoogleSettings]
  lazy val localResources = TableQuery[LocalResources]
  lazy val ugerResources = TableQuery[UgerResources]
  lazy val lsfResources = TableQuery[LsfResources]
  lazy val googleResources = TableQuery[GoogleResources]

  private lazy val allTables: Map[String, SchemaDescription] = Map(
    Names.executions -> executions.schema,
    Names.outputs -> outputs.schema,
    Names.localSettings -> localSettings.schema,
    Names.ugerSettings -> ugerSettings.schema,
    Names.lsfSettings -> lsfSettings.schema,
    Names.googleSettings -> googleSettings.schema,
    Names.localResources -> localResources.schema,
    Names.ugerResources -> ugerResources.schema,
    Names.lsfResources -> lsfResources.schema,
    Names.googleResources -> googleResources.schema
  )

  private def allTableNames: Seq[String] = allTables.keys.toSeq

  private def allSchemas: Seq[SchemaDescription] = allTables.values.toSeq

  private def ddlForAllTables = allSchemas.reduce(_ ++ _)

  def create(database: Database): Unit = {
    //TODO: Is this appropriate?
    implicit val executionContext = database.executor.executionContext

    def existing = {
      val existingTableNames = for {
        tables <- MTable.getTables
      } yield tables.map(_.name.name).toSet

      runBlocking(database)(existingTableNames)
    }

    def createActions(tables: Map[String, SchemaDescription]) = {
      val actions = for {
        (tableName, schema) <- tables
        if !existing.contains(tableName)
      } yield {
        log(schema)

        schema.create
      }

      DBIO.sequence(actions).transactionally
    }

    // Make sure 'Executions' table is created first
    val executionsTable = Map(Names.executions -> allTables(Names.executions))
    
    runBlocking(database)(createActions(executionsTable))
    
    // Then create the others that depend on 'Executions'
    runBlocking(database)(createActions(allTables - Names.executions))
  }

  def drop(database: Database): Unit = runBlocking(database)(ddlForAllTables.drop.transactionally)

  private def log(schema: SchemaDescription): Unit = {
    schema.createStatements.foreach(s => trace(s"DDL: $s"))
  }

  private def jobStatusfromString(str: String): JobStatus = JobStatus.fromString(str).getOrElse {
    warn(s"$str is not one of known JobStatus'es; mapping to ${JobStatus.Unknown}")
    JobStatus.Unknown
  }
}

object Tables {
  object Names {
    val executions = "EXECUTIONS"
    val outputs = "OUTPUTS"
    val localSettings = "SETTINGS_LOCAL"
    val ugerSettings = "SETTINGS_UGER"
    val lsfSettings = "SETTINGS_LSF"
    val googleSettings = "SETTINGS_GOOGLE"
    val localResources = "RESOURCES_LOCAL"
    val ugerResources = "RESOURCES_UGER"
    val lsfResources = "RESOURCES_LSF"
    val googleResources = "RESOURCES_GOOGLE"
  }
}
