package loamstream.db.slick

import java.nio.file.Path

import scala.concurrent.ExecutionContext

import loamstream.db.LoamDao
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Resources
import loamstream.model.execute.Settings
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandInvocationFailure
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.OutputRecord
import loamstream.util.Loggable
import loamstream.util.PathUtils

/**
 * @author clint
 *         kyuksel
 *         date: 8/8/2016
 *
 * LoamDao implementation backed by Slick
 * For a schema description, see Tables
 */
// scalastyle:off number.of.methods
//TODO: Re-enable `number.of.methods` - it's a good indicator that a class should be broken up.
final class SlickLoamDao(val descriptor: DbDescriptor) extends LoamDao with Loggable {
  val driver = descriptor.dbType.driver

  import driver.api._
  import loamstream.util.Futures.waitFor

  private def doFindOutput[A](loc: String, f: OutputRow => A): Option[A] = {
    val action = findOutputAction(loc)

    runBlocking(action).map(f)
  }

  override def findOutputRecord(loc: String): Option[OutputRecord] = findOutputRow(loc).map(toOutputRecord)

  override def findCommand(loc: String): Option[String] = {
    for {
      outputRow <- findOutputRow(loc)
      executionId <- outputRow.executionId
      executionRow <- findExecutionRow(executionId)
    } yield executionRow.cmd
  }

  override def deleteOutput(locs: Iterable[String]): Unit = {
    val delete = outputDeleteAction(locs)

    runBlocking(delete.transactionally)
  }

  override def deletePathOutput(paths: Iterable[Path]): Unit = {
    deleteOutput(paths.map(PathUtils.normalize))
  }

  /**
   * Insert the given ExecutionRow and return what is recorded with the updated (auto-incremented) id
   */
  def insertExecutionRow(executionToRecord: ExecutionRow): DBIO[ExecutionRow] = {
    Queries.insertExecution += executionToRecord
  }

  // TODO Input no longer needs to be a (Execution, JobResult) since Execution contains JobResult now
  private def insert(executionAndResult: (Execution, JobResult.CommandResult)): DBIO[Iterable[Int]] = {
    val (execution, commandResult) = executionAndResult

    import Helpers.dummyId

    require(execution.cmd.isDefined, s"An Execution with a command line defined is required, but got $execution")

    //NB: Note dummy ID, will be assigned an auto-increment ID by the DB :\
    //NB: Also note unsafe .get, which is "ok" here since the executions without command lines will have been
    //filtered out before we get here.
    val executionRow = {
      new ExecutionRow(
        id = dummyId, 
        env = execution.env.tpe.name, 
        cmd = execution.cmd.get,
        status = execution.status, 
        exitCode = commandResult.exitCode,
        stdoutPath = execution.outputStreams.get.stdout.toString,
        stderrPath = execution.outputStreams.get.stderr.toString)
    }

    import Implicits._

    for {
      newExecution <- insertExecutionRow(executionRow)
      outputsWithExecutionId = tieOutputsToExecution(execution, newExecution.id)
      settingsWithExecutionId = tieSettingsToExecution(execution, newExecution.id)
      resourcesWithExecutionId = tieResourcesToExecution(execution, newExecution.id)
      insertedOutputCounts <- insertOrUpdateOutputRows(outputsWithExecutionId)
      insertedSettingCounts <- insertOrUpdateSettingRow(settingsWithExecutionId)
      insertedResourceCounts <- insertOrUpdateResourceRows(resourcesWithExecutionId)
    } yield {
      insertedOutputCounts ++ Iterable(insertedSettingCounts) ++ insertedResourceCounts
    }
  }

  override def insertExecutions(executions: Iterable[Execution]): Unit = {

    requireCommandExecution(executions)

    val inserts = insertableExecutions(executions).map(insert)

    val insertEverything = DBIO.sequence(inserts).transactionally

    runBlocking(insertEverything)
  }

  //TODO: Find way to extract common code from the all* methods
  override def allOutputRecords: Seq[OutputRecord] = {
    val query = tables.outputs.result

    log(query)

    runBlocking(query.transactionally).map(toOutputRecord)
  }

  override def allExecutions: Seq[Execution] = {
    val query = tables.executions.result

    log(query)

    val executions = runBlocking(query.transactionally)

    for {
      execution <- executions
    } yield {
      val outputs = outputsFor(execution)
      val settings = settingsFor(execution)
      val resources = resourcesFor(execution)
      execution.toExecution(settings, resources, outputs.toSet)
    }
  }

  override def findExecution(output: OutputRecord): Option[Execution] = {

    val lookingFor = output.loc

    val executionForPath = for {
      output <- tables.outputs.filter(_.locator === lookingFor)
      execution <- output.execution
    } yield {
      execution
    }

    log(executionForPath.result)

    import Implicits._

    val query = for {
      executionOption <- executionForPath.result.headOption
    } yield executionOption.map(reify)

    runBlocking(query)
  }

  override def createTables(): Unit = tables.create(db)

  override def dropTables(): Unit = tables.drop(db)

  override def shutdown(): Unit = waitFor(db.shutdown)

  private def toOutputRecord(row: OutputRow): OutputRecord = row.toOutputRecord

  private object Implicits {
    //TODO: re-evaluate; does this make sense?
    implicit val dbExecutionContext: ExecutionContext = db.executor.executionContext
  }

  private def findOutputRow(loc: String): Option[OutputRow] = {
    val action = findOutputAction(loc)

    runBlocking(action)
  }

  private def findExecutionRow(executionId: Int): Option[ExecutionRow] = {
    val action = findExecutionAction(executionId)

    runBlocking(action)
  }

  private def findExecutionAction(executionId: Int): DBIO[Option[ExecutionRow]] =
    Queries.executionById(executionId).result.headOption.transactionally

  private def findOutputAction(loc: String): DBIO[Option[OutputRow]] = {
    Queries.outputByLoc(loc).result.headOption.transactionally
  }

  private type WriteAction[A] =  driver.ProfileAction[A, NoStream, Effect.Write]
  
  private def outputDeleteAction(locsToDelete: Iterable[String]): WriteAction[Int] = {
    Queries.outputsByPaths(locsToDelete).delete
  }

  private def outputDeleteActionRaw(pathsToDelete: Iterable[String]): WriteAction[Int] = {
    Queries.outputsByRawPaths(pathsToDelete).delete
  }

  private def insertOrUpdateOutputRows(rows: Iterable[OutputRow]): DBIO[Iterable[Int]] = {
    val insertActions = rows.map(tables.outputs.insertOrUpdate)

    DBIO.sequence(insertActions).transactionally
  }

  private def insertOrUpdateSettingRow(row: SettingRow): DBIO[Int] = row.insertOrUpdate(tables)

  private def insertOrUpdateResourceRow(row: ResourceRow): DBIO[Int] = row.insertOrUpdate(tables)

  private def insertOrUpdateResourceRows(rows: Iterable[ResourceRow]): DBIO[Seq[Int]] = {
    DBIO.sequence(rows.toSeq.map(insertOrUpdateResourceRow))
  }

  private def tieOutputsToExecution(execution: Execution, executionId: Int): Seq[OutputRow] = {
    def toOutputRows(f: OutputRecord => OutputRow): Seq[OutputRow] = {
      execution.outputs.toSeq.map(f)
    }

    val outputs = {
      if(execution.isSuccess) { toOutputRows(new OutputRow(_)) }
      else if(execution.isFailure) { toOutputRows(rec => new OutputRow(rec.loc)) }
      else { Nil }
    }

    outputs.map(_.withExecutionId(executionId))
  }

  private def tieSettingsToExecution(execution: Execution, executionId: Int): SettingRow = {
      SettingRow.fromSettings(execution.settings, executionId)
  }

  private def tieResourcesToExecution(execution: Execution, executionId: Int): Option[ResourceRow] = {
    execution.resources.map(rs => ResourceRow.fromResources(rs, executionId))
  }

  private object Queries {
    lazy val insertExecution: driver.IntoInsertActionComposer[ExecutionRow, ExecutionRow] =
      (tables.executions returning tables.executions.map(_.id)).into {
      (execution, newId) => execution.copy(id = newId)
    }

    def outputByLoc(loc: String): Query[tables.Outputs, OutputRow, Seq] = {
      tables.outputs.filter(_.locator === loc).take(1)
    }

    def outputsByPaths(locs: Iterable[String]): Query[tables.Outputs, OutputRow, Seq] = {
      val rawPaths = locs.toSet

      outputsByRawPaths(rawPaths)
    }

    def outputsByRawPaths(rawPaths: Iterable[String]): Query[tables.Outputs, OutputRow, Seq] = {
      tables.outputs.filter(_.locator.inSetBind(rawPaths))
    }

    def executionById(executionId: Int): Query[tables.Executions, ExecutionRow, Seq] = {
      tables.executions.filter(_.id === executionId).take(1)
    }
  }

  private def reify(executionRow: ExecutionRow): Execution = {
    executionRow.toExecution(settingsFor(executionRow), resourcesFor(executionRow), outputsFor(executionRow).toSet)
  }

  //TODO: There must be a better way than a subquery
  private def outputsFor(execution: ExecutionRow): Seq[OutputRecord] = {
    val query = tables.outputs.filter(_.executionId === execution.id).result

    runBlocking(query).map(toOutputRecord)
  }

  private def settingsFor(execution: ExecutionRow): Settings = {
    //Oh, Slick ... yow :\
    type SettingTable = TableQuery[_ <: tables.driver.api.Table[_ <: SettingRow] with tables.HasExecutionId]

    def settingsFrom(table: SettingTable): Seq[Settings] = {
      runBlocking(table.filter(_.executionId === execution.id).result).map(_.toSettings)
    }

    val table: SettingTable = execution.env match {
      case EnvironmentType.Names.Local => tables.localSettings
      case EnvironmentType.Names.Uger => tables.ugerSettings
      case EnvironmentType.Names.Google => tables.googleSettings
    }

    val queryResults: Seq[Settings] = settingsFrom(table)

    require(queryResults.size == 1,
      s"There must be a single set of settings per execution. " +
        s"Found ${queryResults.size} for the execution with ID '${execution.id}'")

    queryResults.head
  }

  private def resourcesFor(execution: ExecutionRow): Option[Resources] = {
    //Oh, Slick ... yow :\
    type ResourceTable = TableQuery[_ <: tables.driver.api.Table[_ <: ResourceRow] with tables.HasExecutionId]
    
    def resourcesFrom(table: ResourceTable): Seq[Resources] = {
      runBlocking(table.filter(_.executionId === execution.id).result).map(_.toResources)
    }
    
    val table: ResourceTable = execution.env match {
      case EnvironmentType.Names.Local => tables.localResources
      case EnvironmentType.Names.Uger => tables.ugerResources
      case EnvironmentType.Names.Google => tables.googleResources
    }
    
    val queryResults: Seq[Resources] = resourcesFrom(table)
    
    require(queryResults.size <= 1,
      s"There must be at most 1 sets of resource usages per execution. " +
        s"Found ${queryResults.size} for the execution with ID '${execution.id}'")

    queryResults.headOption
  }

  //TODO: Re-evaluate; block all the time?
  private[slick] def runBlocking[A](action: DBIO[A]): A = waitFor(db.run(action))

  private def log(sqlAction: driver.ProfileAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => trace(s"SQL: $s"))
  }

  private def insertableExecutions(executions: Iterable[Execution]): Iterable[(Execution, CommandResult)] = {
    executions.collect {
      case e @ Execution(_, _, _, _, Some(cr: CommandResult), _, _, _) => e -> cr
      //NB: Allow storing the failure to invoke a command; give this case DummyExitCode
      case e @ Execution(_, _, _, _, Some(cr: CommandInvocationFailure), _, _, _) => {
        // TODO: Better assign e -> JobResult.Failure?
        e -> CommandResult(JobResult.DummyExitCode)
      }
    }
  }

  private def requireCommandExecution(executions: Iterable[Execution]): Unit = {
    def firstNonCommandExecution: Execution = executions.find(!_.isCommandExecution).get

    require(
      executions.forall(_.isCommandExecution),
      s"We only know how to record command executions, but we got $firstNonCommandExecution")

    trace(s"INSERTING: $executions")
  }

  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.dbType.jdbcDriverClass)

  private[slick] lazy val tables = new Tables(driver)
}
// scalastyle:on number.of.methods
