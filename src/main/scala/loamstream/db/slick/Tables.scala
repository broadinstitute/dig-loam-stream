package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.jobs.JobStatus
import loamstream.util.Futures
import loamstream.util.Loggable
import slick.jdbc.JdbcProfile
import slick.jdbc.meta.MTable
import slick.ast.ColumnOption
import scala.reflect.ClassTag
import slick.lifted.ForeignKeyQuery
import scala.collection.compat._

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

  trait HasIntId { self: Table[_] =>
    def id: Rep[Int] = column[Int]("ID", O.AutoInc, O.PrimaryKey)
  }

  //Use Int.MaxValue as an approximation of maximum String length, since Strings are represented as
  //char arrays indexed by integers on the JVM, and String.length() returns an int.
  private[this] val maxStringColumnLength = Int.MaxValue
  
  final class Runs(tag: Tag) extends Table[RunRow](tag, Names.runs) with HasIntId {
    def name: Rep[String] = column[String]("NAME")
    def timeStamp: Rep[Timestamp] = column[Timestamp]("RUN_TIME")
    
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = {
      (id, name, timeStamp) <> ((RunRow.apply _).tupled, RunRow.unapply)
    }
  }
  
  final class Executions(tag: Tag) extends Table[ExecutionRow](tag, Names.executions) with HasIntId {
    def env: Rep[String] = column[String]("ENV")
    //NB: Specify the length of this column so that we hopefully don't get a too-small VARCHAR,
    //and instead some DB-specific column type appropriate for strings thousands of chars long.
    def cmd: Rep[Option[String]] = column[Option[String]]("CMD", O.Length(maxStringColumnLength))
    def jobDir: Rep[Option[String]] = column[Option[String]]("JOB_DIR", O.Length(maxStringColumnLength))
    def exitCode: Rep[Int] = column[Int]("EXIT_CODE")
    def status: Rep[JobStatus] = column[JobStatus]("STATUS")
    def terminationReason: Rep[Option[String]] = column[Option[String]]("TERM_REASON")
    
    def runId: Rep[Int] = column[Int]("RUN_ID")
    
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = {
      (id, env, cmd, status, exitCode, jobDir, terminationReason, runId.?) <> 
          (ExecutionRow.tupled, ExecutionRow.unapply)
    }
    
    def run: ForeignKeyQuery[Runs, RunRow] = {
      val foreignKeyName = s"${runForeignKeyPrefix}${Names.executions}"
      
      foreignKey(foreignKeyName, runId, runs)(_.id, onUpdate = Restrict, onDelete = Cascade)
    }
  }

  final class Outputs(tag: Tag) extends Table[OutputRow](tag, Names.outputs) {
    def locator: Rep[String] = column[String]("LOCATOR", O.PrimaryKey)
    def lastModified: Rep[Option[Timestamp]] = column[Option[Timestamp]]("LAST_MODIFIED")
    def hash: Rep[Option[String]] = column[Option[String]]("HASH")
    def hashType: Rep[Option[String]] = column[Option[String]]("HASH_TYPE")
    
    def executionId: Rep[Int] = column[Int]("EXECUTION_ID")
    //NB: Required by Slick to define the mapping between DB columns and case class fields.
    //It's unlikely devs will need to call it directly.
    override def * = (locator, lastModified, hash, hashType, executionId.?) <> (OutputRow.tupled, OutputRow.unapply)
    
    def execution: ForeignKeyQuery[Executions, ExecutionRow] = {
      val foreignKeyName = s"${executionForeignKeyPrefix}${Names.outputs}"
      
      foreignKey(foreignKeyName, executionId, executions)(_.id, onUpdate = Restrict, onDelete = Cascade)
    }
  }
  
  private val runForeignKeyPrefix = s"FK_ID_RUNS_"
  
  private val executionForeignKeyPrefix = s"FK_ID_EXECUTIONS_"
  
  lazy val runs = TableQuery[Runs]
  lazy val executions = TableQuery[Executions]
  lazy val outputs = TableQuery[Outputs]

  //NB: Now a Seq so we can guarantee ordering
  private lazy val allTables: Seq[(String, SchemaDescription)] = Seq(
    Names.runs -> runs.schema,
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
        //NB: Plain old Mtable.getTables doesn't work for HSQLDB.
        //This multi-param overload needs to be called instead.
        //See https://stackoverflow.com/questions/29536689/how-to-query-database-tables-using-slick-and-hsqldb
        MTable.getTables(Some("PUBLIC"), Some("PUBLIC"), Some(tableName), Some(Seq("TABLE"))).headOption
      }
      
      allTableNames.to(Set).flatMap { tableName =>
        val queryForTable = for {
          tableMetadataOpt <- queryForTableMetadata(tableName)
        } yield tableMetadataOpt.map(_.name.name)
        
        runBlocking(database)(queryForTable)
      }
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
    val runs = "RUNS"
    val executions = "EXECUTIONS"
    val outputs = "OUTPUTS"
  }
}
