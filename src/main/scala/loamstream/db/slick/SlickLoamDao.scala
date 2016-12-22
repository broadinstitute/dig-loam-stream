package loamstream.db.slick

import java.nio.file.Path

import scala.concurrent.ExecutionContext
import loamstream.db.LoamDao
import loamstream.model.jobs.{Execution, JobState, OutputRecord}
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.PathUtils
import slick.profile.SqlAction
import loamstream.model.jobs.JobState.CommandInvocationFailure

/**
 * @author clint
 * date: Aug 8, 2016
 * 
 * LoamDao implementation backed by Slick
 * For a schema description, see Tables 
 */
final class SlickLoamDao(val descriptor: DbDescriptor) extends LoamDao with Loggable {
  val driver = descriptor.dbType.driver
  
  import driver.api._
  import Futures.waitFor
  
  private lazy val outputsAndExecutions = tables.outputs.join(tables.executions).on(_.executionId === _.id)
  
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
    
    //NB: Note dummy ID, will be assigned an auto-increment ID by the DB :\
    val rawExecutionRow = new ExecutionRow(dummyId, commandResult.exitStatus)
    
    def toRawOutputRows(f: OutputRecord => OutputRow): Seq[OutputRow] = {
      execution.outputs.toSeq.map(f)
    }
    
    val outputs = {
      if(execution.isSuccess) { toRawOutputRows(new OutputRow(_)) }
      else if(execution.isFailure) { toRawOutputRows(rec => new OutputRow(rec.loc)) }
      else { Nil }
    }
    
    def tieOutputsToExecution(outputs: Seq[OutputRow], executionId: Int): Seq[OutputRow] = {
      outputs.map(_.withExecutionId(executionId))
    }
    
    import Implicits._
    
    for {
      newExecution <- (Queries.insertExecution += rawExecutionRow)
      outputsWithExecutionIds = tieOutputsToExecution(outputs, newExecution.id)
      insertedOutputCounts <- insertOrUpdateRawOutputRows(outputsWithExecutionIds)
    } yield {
      insertedOutputCounts
    }
  }
  
  override def insertExecutions(executions: Iterable[Execution]): Unit = {
    def firstNonCommandExecution: Execution = executions.find(!_.isCommandExecution).get
    
    require(
      executions.forall(_.isCommandExecution), 
      s"We only know how to record command executions, but we got $firstNonCommandExecution")
    
    debug(s"INSERTING: $executions")
    
    import JobState.CommandResult
    
    val insertableExecutions: Iterable[(Execution, CommandResult)] = executions.collect {
      case e @ Execution(cr: CommandResult, _) => e -> cr
      //NB: Allow storing the failure to invoke a command; give this case the dummy "exit code" -1
      case e @ Execution(cr: CommandInvocationFailure, _) => e -> CommandResult(-1)
    } 
    
    val inserts = insertableExecutions.map(insert)
    
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
      
      execution.toExecution(outputs.toSet)
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
  
  private def outputDeleteActionRaw(pathsToDelete: Iterable[String]): SqlAction[Int,NoStream, Effect.Write] = {
    Queries.outputsByRawPaths(pathsToDelete).delete
  }
  
  //TODO: Need to allow updating?
  private def insertOrUpdateOutputs(rows: Iterable[OutputRecord]): Unit = {
    val rawRows = rows.map(new OutputRow(_))

    val insertOrUpdate = insertOrUpdateRawOutputRows(rawRows)
    
    runBlocking(insertOrUpdate)
  }
  
  private def insertOrUpdateRawOutputRows(rawRows: Iterable[OutputRow]): DBIO[Iterable[Int]] = {
    val insertActions = rawRows.map(tables.outputs.insertOrUpdate)
    
    DBIO.sequence(insertActions).transactionally
  }
  
  private object Queries {
    import PathUtils.normalize
    
    lazy val insertExecution = (tables.executions returning tables.executions.map(_.id)).into { 
      (execution, newId) => execution.copy(id = newId)
    }
    
    def outputByLoc(loc: String) = {
      tables.outputs.filter(_.locator === loc).take(1)
    }
    
    def outputsByPaths(locs: Iterable[String]) = {
      val rawPaths = locs.toSet
      
      outputsByRawPaths(rawPaths)
    }
    
    def outputsByRawPaths(rawPaths: Iterable[String]) = {
      tables.outputs.filter(_.locator.inSetBind(rawPaths))
    }
  }
  
  private def reify(executionRow: ExecutionRow): Execution = {
    executionRow.toExecution(outputsFor(executionRow).toSet)
  }
  
  //TODO: There must be a better way than a subquery
  private def outputsFor(execution: ExecutionRow): Seq[OutputRecord] = {
    val query = tables.outputs.filter(_.executionId === execution.id).result
    
    runBlocking(query).map(toOutputRecord)
  }
  
  //TODO: Re-evaluate; block all the time?
  private def runBlocking[A](action: DBIO[A]): A = waitFor(db.run(action))

  private def log(sqlAction: SqlAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => debug(s"SQL: $s"))
  }
  
  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.dbType.jdbcDriverClass)

  private[slick] lazy val tables = new Tables(driver)
}
