package loamstream.db.slick

import java.nio.file.Path

import scala.concurrent.ExecutionContext
import loamstream.db.LoamDao
import loamstream.model.execute._
import loamstream.model.jobs.{Execution, JobState, OutputRecord}
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.PathUtils
import slick.profile.SqlAction
import loamstream.model.jobs.JobState.{CommandInvocationFailure, CommandResult}
import loamstream.model.execute.Resources.LocalResources

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
  import Futures.waitFor

  private def doFindOutput[A](loc: String, f: OutputRow => A): Option[A] = {
    val action = findOutputAction(loc)

    runBlocking(action).map(f)
  }

  override def findOutputRecord(loc: String): Option[OutputRecord] = {
    val action = findOutputAction(loc)

    runBlocking(action).map(toOutputRecord)
  }

  override def deleteOutput(locs: Iterable[String]): Unit = {
    val delete = outputDeleteAction(locs)

    runBlocking(delete.transactionally)
  }

  override def deletePathOutput(paths: Iterable[Path]): Unit = {
    deleteOutput(paths.map(PathUtils.normalize))
  }

  private def insert(executionAndState: (Execution, JobState.CommandResult)): DBIO[Iterable[Int]] = {
    val (execution, commandResult) = executionAndState

    import Helpers.dummyId

    require(execution.cmd.isDefined, s"An Execution with a command line defined is required, but got $execution")
    
    //NB: Note dummy ID, will be assigned an auto-increment ID by the DB :\
    //NB: Also note unsafe .get, which is "ok" here since we executions without command lines will have been
    //filtered out before we get here.  
    val executionRow = new ExecutionRow(dummyId, execution.env.name, execution.cmd.get, commandResult.exitStatus)
    
    import Implicits._

    for {
      newExecution <- (Queries.insertExecution += executionRow)
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

  private def findOutputAction(loc: String): DBIO[Option[OutputRow]] = {
    Queries.outputByLoc(loc).result.headOption.transactionally
  }

  private def outputDeleteAction(locsToDelete: Iterable[String]): SqlAction[Int, NoStream, Effect.Write] = {
    Queries.outputsByPaths(locsToDelete).delete
  }

  private def outputDeleteActionRaw(pathsToDelete: Iterable[String]): SqlAction[Int, NoStream, Effect.Write] = {
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
    import PathUtils.normalize

    lazy val insertExecution = (tables.executions returning tables.executions.map(_.id)).into {
      (execution, newId) => execution.copy(id = newId)
    }

    def outputByLoc(loc: String) = tables.outputs.filter(_.locator === loc).take(1)

    def outputsByPaths(locs: Iterable[String]) = {
      val rawPaths = locs.toSet

      outputsByRawPaths(rawPaths)
    }

    def outputsByRawPaths(rawPaths: Iterable[String]) = tables.outputs.filter(_.locator.inSetBind(rawPaths))
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
    
    val table: SettingTable = ExecutionEnvironment.fromString(execution.env) match {
      case ExecutionEnvironment.Local => tables.localSettings
      case ExecutionEnvironment.Uger => tables.ugerSettings
      case ExecutionEnvironment.Google => tables.googleSettings
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
    
    val table: ResourceTable = ExecutionEnvironment.fromString(execution.env) match {
      case ExecutionEnvironment.Local => tables.localResources
      case ExecutionEnvironment.Uger => tables.ugerResources
      case ExecutionEnvironment.Google => tables.googleResources
    }
    
    val queryResults: Seq[Resources] = resourcesFrom(table)
    
    require(queryResults.size <= 1,
      s"There must be at most 1 sets of resource usages per execution. " +
        s"Found ${queryResults.size} for the execution with ID '${execution.id}'")

    queryResults.headOption
  }

  //TODO: Re-evaluate; block all the time?
  private def runBlocking[A](action: DBIO[A]): A = waitFor(db.run(action))

  private def log(sqlAction: SqlAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => debug(s"SQL: $s"))
  }

  private def insertableExecutions(executions: Iterable[Execution]): Iterable[(Execution, CommandResult)] = {
    executions.collect {
      case e @ Execution(_, _, _, cr: CommandResult, _) => e -> cr
      //NB: Allow storing the failure to invoke a command; give this case the dummy "exit code" -1
      //TODO: Dummy value
      case e @ Execution(_, _, _, cr: CommandInvocationFailure, _) => e -> CommandResult(-1, None)
    }
  }

  private def requireCommandExecution(executions: Iterable[Execution]): Unit = {
    def firstNonCommandExecution: Execution = executions.find(!_.isCommandExecution).get

    require(
      executions.forall(_.isCommandExecution),
      s"We only know how to record command executions, but we got $firstNonCommandExecution")

    debug(s"INSERTING: $executions")
  }

  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.dbType.jdbcDriverClass)

  private[slick] lazy val tables = new Tables(driver)
}
// scalastyle:on number.of.methods
