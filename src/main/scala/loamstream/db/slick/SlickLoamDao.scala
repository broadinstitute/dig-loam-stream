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
  
  private def findOutputAction(path: Path): DBIO[Option[RawOutputRow]] = {
    val lookingFor = Helpers.normalize(path)
    
    tables.outputs.filter(_.path === lookingFor).take(1).result.headOption.transactionally
  }
  
  override def findOutput(path: Path): Option[CachedOutput] = {
    val action = findOutputAction(path)
    
    waitFor(db.run(action)).map(_.toCachedOutput)
  }
  
  private def outputDeleteAction(pathsToDelete: Iterable[Path]): DBIO[Int] = {
    val toDelete = pathsToDelete.map(Helpers.normalize).toSet
    
    outputDeleteActionRaw(toDelete)
  }
  
  private def outputDeleteActionRaw(pathsToDelete: Iterable[String]): DBIO[Int] = {
    tables.outputs.filter(_.path.inSetBind(pathsToDelete)).delete
  }
  
  override def deleteOutput(paths: Iterable[Path]): Unit = {
    val action = outputDeleteAction(paths).transactionally
    
    waitFor(db.run(action))
  }
  
  //TODO: Need to allow updating?
  override def insertOrUpdateOutputs(rows: Iterable[Output.PathBased]): Unit = {
    val rawRows = rows.map(row => new RawOutputRow(row.path, row.hash))

    val action = insertOrUpdateRawOutputRows(rawRows)
    
    waitFor(db.run(action))
  }
  
  private def insertOrUpdateRawOutputRows(rawRows: Iterable[RawOutputRow]): DBIO[_] = {
    val paths = rawRows.map(_.pathValue)
    
    val insertAction = tables.outputs ++= rawRows
    
    DBIO.seq(outputDeleteActionRaw(paths), insertAction).transactionally
  }
  
  override def insertExecutions(executions: Iterable[Execution]): Unit = {
    def insert(execution: Execution): DBIO[_] = {
      val rawExecutionRow = new RawExecutionRow(None, execution.exitStatus)
      
      val executionIdQuery = (tables.executions returning tables.executions.map(_.id)) += rawExecutionRow
      
      val pathBasedOutputs = execution.outputs.collect { case pb: Output.PathBased => pb }
      
      val outputs = pathBasedOutputs.map(new RawOutputRow(_))
      
      implicit val context = db.executor.executionContext
      
      for {
        newId <- executionIdQuery
      } yield {
        val outputsWithExecutionIds = outputs.map(_.copy(executionId = Option(newId)))
        
        insertOrUpdateRawOutputRows(outputsWithExecutionIds)
      }
    }
    
    val inserts = DBIO.sequence(executions.map(insert)).transactionally
    
    waitFor(db.run(inserts))
  }
  
  override def allOutputRows: Seq[CachedOutput] = {
    val query = tables.outputs.result.transactionally
    
    val futureRow = db.run(query)
    
    //TODO: Re-evaluate
    waitFor(futureRow).map(_.toCachedOutput)
  }
  
  override def allExecutions: Seq[Execution] = {
    val query = tables.executions.result.transactionally
    
    val executions = waitFor(db.run(query))
    
    for {
      execution <- executions
    } yield {
      val outputs = outputsFor(execution)
      
      execution.toExecution(outputs.toSet)
    }
  }
  
  //TODO: This is a total mess
  override def findExecution(output: Output.PathBased): Option[Execution] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val lookingFor = Helpers.normalize(output.path)
    
    val query = for {
      outputRowOption <- tables.outputs.filter(_.path === lookingFor).result.headOption
      if outputRowOption.isDefined
      output = outputRowOption.get
      executionIdOption = output.executionId
      if executionIdOption.isDefined
      executionId = executionIdOption.get
      executionRow <- tables.executions.filter(_.id === executionId).result.headOption
    } yield {
      executionRow.map(ex => ex.toExecution(outputsFor(ex).toSet))
    }
    
    waitFor(db.run(query))
  }
  
  //TODO: There must be a better way than a subquery
  private def outputsFor(execution: RawExecutionRow): Seq[Output] = {
    val query = tables.outputs.filter(_.executionId === execution.id).result
    
    waitFor(db.run(query)).map(_.toCachedOutput)
  }
  
  override def createTables(): Unit = tables.create(db)
  
  override def dropTables(): Unit = tables.drop(db)
  
  override def shutdown(): Unit = waitFor(db.shutdown)
  
  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.dbType.jdbcDriverClass)

  private[slick] lazy val tables = new Tables(driver)
}