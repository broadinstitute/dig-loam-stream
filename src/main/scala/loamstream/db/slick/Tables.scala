package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.jobs.JobStatus
import loamstream.util.Futures
import loamstream.util.Loggable
import slick.jdbc.JdbcProfile
import slick.jdbc.meta.MTable
import slick.ast.ColumnOption
import scala.reflect.ClassTag

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
    final def execution = {
      val foreignKeyName = s"${executionForeignKeyPrefix}${name}"
      
      foreignKey(foreignKeyName, executionId, executions)(_.id, onUpdate = Restrict, onDelete = Cascade)
    }
  }
  
  //Use Int.MaxValue as an approximation of maximum String length, since Strings are represented as
  //char arrays indexed by integers on the JVM, and String.length() returns an int.
  private[this] val maxStringColumnLength = Int.MaxValue
  
  final class Executions(tag: Tag) extends Table[ExecutionRow](tag, Names.executions) {
    def id = column[Int]("ID", O.AutoInc, O.PrimaryKey)
    def env = column[String]("ENV")
    //NB: Specify the length of this column so that we hopefully don't get a too-small VARCHAR,
    //and instead some DB-specific column type appropriate for strings thousands of chars long.
    def cmd = column[Option[String]]("CMD", O.Length(maxStringColumnLength))
    def jobDir = column[Option[String]]("JOB_DIR", O.Length(maxStringColumnLength))
    def exitCode = column[Int]("EXIT_CODE")
    def status = column[JobStatus]("STATUS")
    def terminationReason = column[Option[String]]("TERM_REASON")
    
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = {
      (id, env, cmd, status, exitCode, jobDir, terminationReason) <> (ExecutionRow.tupled, ExecutionRow.unapply)
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
  
  private val executionForeignKeyPrefix = s"FK_ID_EXECUTIONS_"
  
  private val drmSettingsForeignKeyPrefix = s"FK_ID_DRM_SETTINGS_"

  lazy val executions = TableQuery[Executions]
  lazy val outputs = TableQuery[Outputs]

  //NB: Now a Seq so we can guarantee ordering
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

    val existingTableNames: Set[String] = {
      def queryForTableMetadata(tableName: String) = {
        MTable.getTables(Some("PUBLIC"), Some("PUBLIC"), Some(tableName), Some(Seq("TABLE"))).headOption
      }
      
      val tableNames = allTables.toMap.keys.flatMap { tableName =>
        val queryForTable = for {
          //NB: Plain old Mtable.getTables doesn't work for HSQLDB
          //This multi-param overload needs to be called instead.
          //See https://stackoverflow.com/questions/29536689/how-to-query-database-tables-using-slick-and-hsqldb
          tableMetadataOpt <- queryForTableMetadata(tableName)
        } yield tableMetadataOpt.map(_.name.name)
        
        runBlocking(database)(queryForTable)
      }
      
      tableNames.toSet
    }

    def createActions(tables: Seq[(String, SchemaDescription)]) = {
      val tablesToCreateWithSchemas = allTables.toMap -- existingTableNames
      
      val actions = for {
        (tableName, schema) <- tablesToCreateWithSchemas
      } yield {
        log(schema)

        schema.create
      }

      DBIO.sequence(actions).transactionally
    }

    runBlocking(database)(createActions(allTables))
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
  }
}
