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

/**
 * @author clint
 * date: Aug 8, 2016
 * 
 * Rough-draft LoamDao implementation backed by Slick 
 */
final class SlickLoamDao(val descriptor: DbDescriptor) extends LoamDao {
  val driver = descriptor.dbType.driver
  
  import driver.api._
  import Futures.waitFor
  
  private lazy val outputsAndExecutions = tables.outputs.join(tables.executions).on(_.executionId === _.id)
  
  private def findOutputAction(path: Path): DBIO[Option[RawOutputRow]] = {
    Queries.outputByPath(path).result.headOption.transactionally
  }
  
  override def findOutput(path: Path): Option[CachedOutput] = {
    val action = findOutputAction(path)
    
    runBlocking(action).map(_.toCachedOutput)
  }
  
  private def outputDeleteAction(pathsToDelete: Iterable[Path]): SqlAction[Int, NoStream, Effect.Write] = {
    Queries.outputsByPaths(pathsToDelete).delete
  }
  
  private def outputDeleteActionRaw(pathsToDelete: Iterable[String]): SqlAction[Int,NoStream, Effect.Write] = {
    Queries.outputsByRawPaths(pathsToDelete).delete
  }
  
  override def deleteOutput(paths: Iterable[Path]): Unit = {
    val delete = outputDeleteAction(paths)
    
    runBlocking(delete.transactionally)
  }
  
  //TODO: Need to allow updating?
  private def insertOrUpdateOutputs(rows: Iterable[Output.PathBased]): Unit = {
    val rawRows = rows.map(row => new RawOutputRow(row.path, row.hash))

    val insertOrUpdate = insertOrUpdateRawOutputRows(rawRows)
    
    runBlocking(insertOrUpdate)
  }
  
  private def insertOrUpdateRawOutputRows(rawRows: Iterable[RawOutputRow]): DBIO[Iterable[Int]] = {
    println(s"insertOrUpdateRawOutputRows(): inserting: $rawRows")
    
    /*val paths = rawRows.map(_.pathValue)
    
    val insertAction = tables.outputs ++= rawRows
    
    log(insertAction)
    
    DBIO.seq(outputDeleteActionRaw(paths), insertAction).transactionally*/
    
    DBIO.sequence(rawRows.map(tables.outputs.insertOrUpdate)).transactionally
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
  
  override def insertExecutions(executions: Iterable[Execution]): Unit = {
    def insert(execution: Execution): DBIO[Iterable[Int]] = {
      //NB: Note dummy ID :\
      val rawExecutionRow = new RawExecutionRow(-1, execution.exitStatus)

      println(s"insertExecutions(): Inserting: $rawExecutionRow")
      
      val pathBasedOutputs = execution.outputs.collect { case pb: Output.PathBased => pb }
      
      val outputs = pathBasedOutputs.map(new RawOutputRow(_))
      
      implicit val context = db.executor.executionContext
      
      for {
        newExecution <- Queries.insertExecution += rawExecutionRow
        _ = log(Queries.insertExecution += rawExecutionRow)
        _ = println(s"NEW EXECUTION ID: '${newExecution.id}'")
        outputsWithExecutionIds = outputs.map(_.withExecutionId(newExecution.id))
        insertedOutputCounts <- insertOrUpdateRawOutputRows(outputsWithExecutionIds)
      } yield {
        insertedOutputCounts
      }
    }
    
    val inserts = DBIO.sequence(executions.map(insert)).transactionally
    
    runBlocking(inserts)
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
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val lookingFor = Helpers.normalize(output.path)
    
    val executionForPath = for {
      output <- tables.outputs.filter(_.path === lookingFor)
      execution <- output.execution
    } yield {
      execution
    }
    
    log(executionForPath.result)
    
    val query = for {
      executionOption <- executionForPath.result.headOption
    } yield executionOption.map(reify)
    
    runBlocking(query)
  }
  
  private def reify(executionRow: RawExecutionRow): Execution = {
    executionRow.toExecution(outputsFor(executionRow).toSet)
  }
  
  //TODO: There must be a better way than a subquery
  private def outputsFor(execution: RawExecutionRow): Seq[Output] = {
    val query = tables.outputs.filter(_.executionId === execution.id).result
    
    runBlocking(query).map(_.toCachedOutput)
  }
  
  //TODO: Re-evaluate
  private def runBlocking[A](action: DBIO[A]): A = waitFor(db.run(action))

  //TODO
  private def log(sqlAction: SqlAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => println(s"SQL: $s"))
  }
  
  override def createTables(): Unit = tables.create(db)
  
  override def dropTables(): Unit = tables.drop(db)
  
  override def shutdown(): Unit = waitFor(db.shutdown)
  
  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.dbType.jdbcDriverClass)

  private[slick] lazy val tables = new Tables(driver)
}