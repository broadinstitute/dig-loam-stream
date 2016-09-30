package loamstream.db.slick

import java.nio.file.Path

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import loamstream.db.LoamDao
import loamstream.util.Hash
import slick.driver.JdbcProfile
import java.sql.ResultSet
import java.sql.Timestamp
import loamstream.model.jobs.Output.CachedOutput
import scala.util.Try
import slick.jdbc.meta.MTable
import loamstream.util.Futures
import loamstream.model.jobs.Execution
import loamstream.model.jobs.Output
import slick.jdbc.GetResult
import slick.profile.SqlAction
import loamstream.util.Loggable
import scala.concurrent.ExecutionContext
import loamstream.model.jobs.JobState

/**
 * @author clint
 * date: Aug 8, 2016
 * 
 * Rough-draft LoamDao implementation backed by Slick 
 */
final class SlickLoamDao(val descriptor: DbDescriptor) extends LoamDao with Loggable {
  val driver = descriptor.dbType.driver
  
  import driver.api._
  import Futures.waitFor
  
  private lazy val outputsAndExecutions = tables.outputs.join(tables.executions).on(_.executionId === _.id)
  
  override def findOutput(path: Path): Option[CachedOutput] = {
    val action = findOutputAction(path)
    
    runBlocking(action).map(_.toCachedOutput)
  }

  override def deleteOutput(paths: Iterable[Path]): Unit = {
    val delete = outputDeleteAction(paths)
    
    runBlocking(delete.transactionally)
  }
  
  override def insertExecutions(executions: Iterable[Execution]): Unit = {
    assert(executions.forall(isCommandExecution), "We only know how to record command executions")
    
    import JobState.CommandResult
    
    def insert(executionAndState: (Execution, CommandResult)): DBIO[Iterable[Int]] = {
      val (execution, commandResult) = executionAndState
      
      val dummyId = -1
      
      //NB: Note dummy ID, will be assigned an auto-increment ID by the DB :\
      val rawExecutionRow = new RawExecutionRow(dummyId, commandResult.exitStatus)
      
      val pathBasedOutputs = execution.outputs.collect { case pb: Output.PathBased => pb }
      
      val outputs = pathBasedOutputs.map(new RawOutputRow(_))
      
      val insertExectionQuery = Queries.insertExecution += rawExecutionRow

      import Implicits._
      
      for {
        newExecution <- (Queries.insertExecution += rawExecutionRow)
        outputsWithExecutionIds = outputs.map(_.withExecutionId(newExecution.id))
        insertedOutputCounts <- insertOrUpdateRawOutputRows(outputsWithExecutionIds)
      } yield {
        insertedOutputCounts
      }
    }
    
    val insertableExecutions: Iterable[(Execution, CommandResult)] = executions.collect {
      case e @ Execution(cr: JobState.CommandResult, _) => e -> cr
    } 
    
    val inserts = insertableExecutions.map(insert)
    
    val insertEverything = DBIO.sequence(inserts).transactionally
    
    runBlocking(insertEverything)
  }
  
  override def allOutputs: Seq[CachedOutput] = {
    val query = tables.outputs.result
    
    log(query)
    
    runBlocking(query.transactionally).map(_.toCachedOutput)
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
  
  override def findExecution(output: Output.PathBased): Option[Execution] = {
    
    val lookingFor = Helpers.normalize(output.path)
    
    val executionForPath = for {
      output <- tables.outputs.filter(_.path === lookingFor)
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

  private def isCommandExecution(e: Execution): Boolean = {
    import JobState._
      
    e.exitState match {
      case CommandResult(_) => true
      case _ => false
    }
  }
  
  private object Implicits {
    //TODO: re-evaluate; does this make sense?
    implicit val dbExecutionContext: ExecutionContext = db.executor.executionContext
  }
  
  private def findOutputAction(path: Path): DBIO[Option[RawOutputRow]] = {
    Queries.outputByPath(path).result.headOption.transactionally
  }
  
  private def outputDeleteAction(pathsToDelete: Iterable[Path]): SqlAction[Int, NoStream, Effect.Write] = {
    Queries.outputsByPaths(pathsToDelete).delete
  }
  
  private def outputDeleteActionRaw(pathsToDelete: Iterable[String]): SqlAction[Int,NoStream, Effect.Write] = {
    Queries.outputsByRawPaths(pathsToDelete).delete
  }
  
  //TODO: Need to allow updating?
  private def insertOrUpdateOutputs(rows: Iterable[Output.PathBased]): Unit = {
    val rawRows = rows.map(row => new RawOutputRow(row.path, row.hash))

    val insertOrUpdate = insertOrUpdateRawOutputRows(rawRows)
    
    runBlocking(insertOrUpdate)
  }
  
  private def insertOrUpdateRawOutputRows(rawRows: Iterable[RawOutputRow]): DBIO[Iterable[Int]] = {
    val insertActions = rawRows.map(tables.outputs.insertOrUpdate)
    
    DBIO.sequence(insertActions).transactionally
  }
  
  private object Queries {
    lazy val insertExecution = (tables.executions returning tables.executions.map(_.id)).into { 
      (execution, newId) => execution.copy(id = newId)
    }
    
    def outputByPath(path: Path) = { 
      val lookingFor = Helpers.normalize(path)

      tables.outputs.filter(_.path === lookingFor).take(1)
    }
    
    def outputsByPaths(paths: Iterable[Path]) = {
      val rawPaths = paths.map(Helpers.normalize).toSet
      
      outputsByRawPaths(rawPaths)
    }
    
    def outputsByRawPaths(rawPaths: Iterable[String]) = {
      tables.outputs.filter(_.path.inSetBind(rawPaths))
    }
  }
  
  private def reify(executionRow: RawExecutionRow): Execution = {
    executionRow.toExecution(outputsFor(executionRow).toSet)
  }
  
  //TODO: There must be a better way than a subquery
  private def outputsFor(execution: RawExecutionRow): Seq[Output] = {
    val query = tables.outputs.filter(_.executionId === execution.id).result
    
    runBlocking(query).map(_.toCachedOutput)
  }
  
  //TODO: Re-evaluate; block all the time?
  private def runBlocking[A](action: DBIO[A]): A = waitFor(db.run(action))

  private def log(sqlAction: SqlAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => debug(s"SQL: $s"))
  }
  
  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.dbType.jdbcDriverClass)

  private[slick] lazy val tables = new Tables(driver)
}