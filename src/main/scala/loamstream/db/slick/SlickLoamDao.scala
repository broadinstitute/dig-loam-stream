package loamstream.db.slick

import java.nio.file.Path

import scala.concurrent.ExecutionContext

import loamstream.db.LoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobState
import loamstream.model.jobs.Output
import loamstream.model.jobs.Output.CachedOutput
import loamstream.model.jobs.Output.PathOutput
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.PathUtils
import slick.profile.SqlAction

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
  
  override def findFailedOutput(path: Path): Option[PathOutput] = {
    val action = findFailedOutputAction(path)
    
    runBlocking(action).map(_.toPathOutput)
  }

  override def deleteOutput(paths: Iterable[Path]): Unit = {
    val delete = outputDeleteAction(paths)
    
    runBlocking(delete.transactionally)
  }
  
  override def deleteFailedOutput(paths: Iterable[Path]): Unit = {
    val delete = failedOutputDeleteAction(paths)
    
    runBlocking(delete.transactionally)
  }
  
  override def insertExecutions(executions: Iterable[Execution]): Unit = {
    def firstNonCommandExecution: Execution = executions.find(!_.isCommandExecution).get
    
    require(
      executions.forall(_.isCommandExecution), 
      s"We only know how to record command executions, but we got $firstNonCommandExecution")
    
    debug(s"INSERTING: $executions")
    
    import JobState.CommandResult
    
    def insert(executionAndState: (Execution, CommandResult)): DBIO[Iterable[Int]] = {
      val (execution, commandResult) = executionAndState
      
      val dummyId = -1
      
      //NB: Note dummy ID, will be assigned an auto-increment ID by the DB :\
      val rawExecutionRow = new RawExecutionRow(dummyId, commandResult.exitStatus)
      
      val pathBasedOutputs = execution.outputs.collect { case pb: Output.PathBased => pb }
      
      val outputs = if(execution.isSuccess) pathBasedOutputs.toSeq.map(new RawOutputRow(_)) else Nil
      
      val failedOutputs = {
        if(execution.isFailure) { pathBasedOutputs.toSeq.map(pb => new FailedOutputRow(pb.path.toString, dummyId)) }
        else { Nil }
      }
      
      def tieOutputsToExecution(outputs: Seq[RawOutputRow], executionId: Int): Seq[RawOutputRow] = {
        outputs.map(_.withExecutionId(executionId))
      }
      
      def tieFailedOutputsToExecution(failedOutputs: Seq[FailedOutputRow], executionId: Int): Seq[FailedOutputRow] = {
        failedOutputs.map(_.withExecutionId(executionId))
      }
      
      import Implicits._
      
      for {
        newExecution <- (Queries.insertExecution += rawExecutionRow)
        outputsWithExecutionIds = tieOutputsToExecution(outputs, newExecution.id)
        failedOutputsWithExecutionIds = tieFailedOutputsToExecution(failedOutputs, newExecution.id)
        insertedOutputCounts <- insertOrUpdateRawOutputRows(outputsWithExecutionIds)
        insertedFailedOutputCounts <- insertOrUpdateFailedOutputRows(failedOutputsWithExecutionIds)
      } yield {
        insertedOutputCounts ++ insertedFailedOutputCounts
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
  
  override def allFailedOutputs: Seq[PathOutput] = {
    val query = tables.failedOutputs.result
    
    log(query)
    
    runBlocking(query.transactionally).map(_.toPathOutput)
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
    
    val lookingFor = PathUtils.normalize(output.path)
    
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

  private object Implicits {
    //TODO: re-evaluate; does this make sense?
    implicit val dbExecutionContext: ExecutionContext = db.executor.executionContext
  }
  
  private def findOutputAction(path: Path): DBIO[Option[RawOutputRow]] = {
    Queries.outputByPath(path).result.headOption.transactionally
  }
  
  private def findFailedOutputAction(path: Path): DBIO[Option[FailedOutputRow]] = {
    Queries.failedOutputByPath(path).result.headOption.transactionally
  }
  
  private def outputDeleteAction(pathsToDelete: Iterable[Path]): SqlAction[Int, NoStream, Effect.Write] = {
    Queries.outputsByPaths(pathsToDelete).delete
  }
  
  private def failedOutputDeleteAction(pathsToDelete: Iterable[Path]): SqlAction[Int, NoStream, Effect.Write] = {
    Queries.failedOutputsByPaths(pathsToDelete).delete
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
  
  private def insertOrUpdateFailedOutputRows(rawRows: Iterable[FailedOutputRow]): DBIO[Iterable[Int]] = {
    val insertActions = rawRows.map(tables.failedOutputs.insertOrUpdate)
    
    DBIO.sequence(insertActions).transactionally
  }
  
  private object Queries {
    import PathUtils.normalize
    
    lazy val insertExecution = (tables.executions returning tables.executions.map(_.id)).into { 
      (execution, newId) => execution.copy(id = newId)
    }
    
    def outputByPath(path: Path) = { 
      val lookingFor = normalize(path)

      tables.outputs.filter(_.path === lookingFor).take(1)
    }
    
    def failedOutputByPath(path: Path) = { 
      val lookingFor = normalize(path)

      tables.failedOutputs.filter(_.path === lookingFor).take(1)
    }
    
    def outputsByPaths(paths: Iterable[Path]) = {
      val rawPaths = paths.map(normalize).toSet
      
      outputsByRawPaths(rawPaths)
    }
    
    def failedOutputsByPaths(paths: Iterable[Path]) = {
      val rawPaths = paths.map(normalize).toSet
      
      failedOutputsByRawPaths(rawPaths)
    }
    
    def outputsByRawPaths(rawPaths: Iterable[String]) = {
      tables.outputs.filter(_.path.inSetBind(rawPaths))
    }
    
    def failedOutputsByRawPaths(rawPaths: Iterable[String]) = {
      tables.failedOutputs.filter(_.path.inSetBind(rawPaths))
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