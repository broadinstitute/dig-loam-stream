package loamstream.db.slick

import slick.jdbc.JdbcProfile
import loamstream.db.LoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.StoreRecord
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobResult.CommandInvocationFailure
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Resources
import loamstream.model.execute.Settings
import java.nio.file.Paths
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.UgerDrmSettings
import loamstream.drm.ContainerParams


/**
 * @author clint
 * Dec 7, 2017
 * 
 * NB: Factored out of SlickLoamDao, which had gotten huge
 */
trait ExecutionDaoOps extends LoamDao { self: CommonDaoOps with OutputDaoOps =>
  def descriptor: DbDescriptor
  
  val driver: JdbcProfile

  import driver.api._
  
  /**
   * Insert the given ExecutionRow and return what is recorded with the updated (auto-incremented) id
   */
  def insertExecutionRow(executionToRecord: ExecutionRow): DBIO[ExecutionRow] = {
    ExecutionQueries.insertExecution += executionToRecord
  }

  override def insertExecutions(executions: Iterable[Execution]): Unit = {

    requireCommandExecution(executions)

    val inserts = insertableExecutions(executions).map(insert)

    val insertEverything = DBIO.sequence(inserts).transactionally

    runBlocking(insertEverything)
  }
  
  override def allExecutions: Seq[Execution] = {
    val query = tables.executions.result

    log(query)

    val executions = runBlocking(query.transactionally)

    executions.map(reify)
  }

  override def findExecution(outputLocation: String): Option[Execution] = {

    val executionForPath = for {
      output <- tables.outputs.filter(_.locator === outputLocation)
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
        env = execution.settings.envType.name, 
        cmd = execution.cmd.get,
        status = execution.status, 
        exitCode = commandResult.exitCode,
        stdoutPath = execution.outputStreams.get.stdout.toString,
        stderrPath = execution.outputStreams.get.stderr.toString,
        terminationReason = execution.terminationReason.map(_.name))
    }

    import Implicits._

    for {
      newExecution <- insertExecutionRow(executionRow)
      outputsWithExecutionId = tieOutputsToExecution(execution, newExecution.id)
      settingsWithExecutionId = tieSettingsToExecution(execution, newExecution.id)
      containerRowsTuple = tieContainerParamsToExecution(execution, newExecution.id)
      resourcesWithExecutionId = tieResourcesToExecution(execution, newExecution.id)
      insertedOutputCounts <- insertOrUpdateOutputRows(outputsWithExecutionId)
      insertedSettingCounts <- insertOrUpdateSettingRow(settingsWithExecutionId)
      insertedContainerSettingsCounts <- insertOrUpdateContainerSettingsRows(containerRowsTuple)
      insertedResourceCounts <- insertOrUpdateResourceRows(resourcesWithExecutionId)
    } yield {
      insertedOutputCounts ++ 
      Iterable(insertedSettingCounts) ++ 
      insertedResourceCounts ++ 
      insertedContainerSettingsCounts
    }
  }
  
  protected def findExecutionRow(executionId: Int): Option[ExecutionRow] = {
    val action = findExecutionAction(executionId)

    runBlocking(action)
  }

  private def findExecutionAction(executionId: Int): DBIO[Option[ExecutionRow]] = {
    ExecutionQueries.executionById(executionId).result.headOption.transactionally
  }
  
  private def requireCommandExecution(executions: Iterable[Execution]): Unit = {
    def firstNonCommandExecution: Execution = executions.find(!_.isCommandExecution).get

    require(
      executions.forall(_.isCommandExecution),
      s"We only know how to record command executions, but we got $firstNonCommandExecution")

    trace(s"INSERTING: $executions")
  }
  
  private def insertableExecutions(executions: Iterable[Execution]): Iterable[(Execution, CommandResult)] = {
    executions.collect {
      case e @ Execution.WithCommandResult(cr) => e -> cr
      //NB: Allow storing the failure to invoke a command; give this case DummyExitCode
      case e @ Execution.WithCommandInvocationFailure(cif) => {
        // TODO: Better to assign e -> JobResult.Failure?
        e -> CommandResult(JobResult.DummyExitCode)
      }
    }
  }
  
  private def reify(executionRow: ExecutionRow): Execution = {
    executionRow.toExecution(settingsFor(executionRow), resourcesFor(executionRow), outputsFor(executionRow).toSet)
  }
  
  //TODO: There must be a better way than a subquery
  private def outputsFor(execution: ExecutionRow): Seq[StoreRecord] = {
    val query = tables.outputs.filter(_.executionId === execution.id).result

    runBlocking(query).map(toOutputRecord)
  }
  
  private type SettingTable[Row] = 
    TableQuery[_ <: tables.driver.api.Table[_ <: Row] with tables.BelongsToExecution[Row]] 
  
  private type ContainerSettingTable[Row] = 
    TableQuery[_ <: tables.driver.api.Table[_ <: Row] with tables.BelongsToDrmSettings[Row, _, _]]  
  
  private def lsfSettingsFor(execution: ExecutionRow): Settings = {
    drmSettingsFor(execution, tables.lsfSettings, tables.lsfContainerSettings)
  }
  
  private def ugerSettingsFor(execution: ExecutionRow): Settings = {
    drmSettingsFor(execution, tables.ugerSettings, tables.ugerContainerSettings)
  }
  
  private def drmSettingsFor[R <: DrmSettingRow with HasContainerParamsToSettings, CR <: ContainerSettingsRow](
    execution: ExecutionRow, 
    settingTable: SettingTable[R],
    containerSettingTable: ContainerSettingTable[CR]): Settings = {
    
    //NB: There must be a better way than all these sequential, blocking subqueries. :(
    
    import scala.language.existentials
    
    val settingsQuery = settingTable.filter(_.executionId === execution.id).take(2)
    
    val settingsRows = runBlocking(settingsQuery.result)
    
    require(
        settingsRows.size == 1,
        s"There must be a single set of settings per execution. " +
          s"Found ${settingsRows.size} for the execution with ID '${execution.id}'")
    
    val settingsRow = settingsRows.head
    
    val containerSettingsQuery = containerSettingTable.filter(_.drmSettingsId === settingsRow.executionId)
    
    val containerSettingsRowOpt = runBlocking(containerSettingsQuery.result.headOption)
    
    val containerParamsOpt = containerSettingsRowOpt.map(_.toContainerParams)
    
    settingsRow.toSettings(containerParamsOpt)
  }

  private def settingsFor(execution: ExecutionRow): Settings = {
    execution.env match {
      case EnvironmentType.Names.Lsf => lsfSettingsFor(execution)
      case EnvironmentType.Names.Uger => ugerSettingsFor(execution)
      case _ => {
        //Oh, Slick ... yow :\
        type SimpleSettingRow = SettingRow with HasSimpleToSettings
        type SettingTable = TableQuery[_ <: tables.driver.api.Table[_ <: SimpleSettingRow] with tables.HasExecutionId]
        
        def settingsFrom(table: SettingTable): Seq[Settings] = {
          runBlocking(table.filter(_.executionId === execution.id).result).map(_.toSettings)
        }
    
        val table: SettingTable = execution.env match {
          case EnvironmentType.Names.Local => tables.localSettings
          case EnvironmentType.Names.Google => tables.googleSettings
        }
    
        val queryResults: Seq[Settings] = settingsFrom(table)
    
        require(queryResults.size == 1,
          s"There must be a single set of settings per execution. " +
            s"Found ${queryResults.size} for the execution with ID '${execution.id}'")
    
        queryResults.head
      }
    }
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
      case EnvironmentType.Names.Lsf => tables.lsfResources
      case EnvironmentType.Names.Google => tables.googleResources
    }
    
    val queryResults: Seq[Resources] = resourcesFrom(table)
    
    require(queryResults.size <= 1,
      s"There must be at most 1 sets of resource usages per execution. " +
        s"Found ${queryResults.size} for the execution with ID '${execution.id}'")

    queryResults.headOption
  }
  
  private def insertOrUpdateSettingRow(row: SettingRow): DBIO[Int] = row.insertOrUpdate(tables)
  
  private def insertOrUpdateResourceRow(row: ResourceRow): DBIO[Int] = row.insertOrUpdate(tables)
  
  private def insertOrUpdateContainerSettingsRows(
      containerSettingsRowOpt: Option[ContainerSettingsRow]): DBIO[Option[Int]] = {
    
    containerSettingsRowOpt match {
      case None => DBIO.successful(None)
      case Some(containerSettingsRow) => {
        import Implicits._
        
        for {
          settingsRowInsertionResult <- containerSettingsRow.insertOrUpdate(tables)
        } yield {
          Option(settingsRowInsertionResult)
        }
      }
    }
  }

  private def insertOrUpdateResourceRows(rows: Option[ResourceRow]): DBIO[Seq[Int]] = {
    DBIO.sequence(rows.toSeq.map(insertOrUpdateResourceRow))
  }

  private def tieOutputsToExecution(execution: Execution, executionId: Int): Seq[OutputRow] = {
    def toOutputRows(f: StoreRecord => OutputRow): Seq[OutputRow] = {
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
  
  private def tieContainerParamsToExecution(
      execution: Execution, 
      executionId: Int): Option[ContainerSettingsRow] = {
    
    execution.settings match {
      case drmSettings: DrmSettings => {
        ContainerSettingsRowCompanion.fromContainerParams(executionId, drmSettings)
      }
      case _ => None 
    }
  }

  private def tieResourcesToExecution(execution: Execution, executionId: Int): Option[ResourceRow] = {
    execution.resources.map(rs => ResourceRow.fromResources(rs, executionId))
  }
  
  private object ExecutionQueries {
    lazy val insertExecution: driver.IntoInsertActionComposer[ExecutionRow, ExecutionRow] =
      (tables.executions returning tables.executions.map(_.id)).into {
      (execution, newId) => execution.copy(id = newId)
    }

    def executionById(executionId: Int): Query[tables.Executions, ExecutionRow, Seq] = {
      tables.executions.filter(_.id === executionId).take(1)
    }
  }
}
