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
        env = execution.envType.name,
        cmd = execution.cmd,
        status = execution.status, 
        exitCode = commandResult.exitCode,
        jobDir = execution.jobDir.map(_.toString),
        terminationReason = execution.terminationReason.map(_.name))
    }

    import Implicits._

    for {
      newExecution <- insertExecutionRow(executionRow)
      outputsWithExecutionId = tieOutputsToExecution(execution, newExecution.id)
      resourcesWithExecutionId = tieResourcesToExecution(execution, newExecution.id)
      insertedOutputCounts <- insertOrUpdateOutputRows(outputsWithExecutionId)
      insertedResourceCounts <- insertOrUpdateResourceRows(resourcesWithExecutionId)
    } yield {
      insertedOutputCounts ++ insertedResourceCounts
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
    executionRow.toExecution(resourcesFor(executionRow), outputsFor(executionRow).toSet)
  }
  
  //TODO: There must be a better way than a subquery
  private def outputsFor(execution: ExecutionRow): Seq[StoreRecord] = {
    val query = tables.outputs.filter(_.executionId === execution.id).result

    runBlocking(query).map(toOutputRecord)
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
  
  private def insertOrUpdateResourceRow(row: ResourceRow): DBIO[Int] = row.insertOrUpdate(tables)
  
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
