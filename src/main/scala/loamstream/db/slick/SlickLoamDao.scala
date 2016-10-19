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
  
  private def doFindOutput[A](path: Path, f: OutputRow => A): Option[A] = {
    val action = findOutputAction(path)
    
    runBlocking(action).map(f)
  }
  
  override def findOutput(path: Path): Option[Output.PathBased] = {
    val action = findOutputAction(path)
    
    runBlocking(action).map(toOutput)
  }
  
  override def findHashedOutput(path: Path): Option[CachedOutput] = {
    val action = findOutputAction(path)
    
    runBlocking(action).flatMap(toCachedOutput)
  }
  
  override def findFailedOutput(path: Path): Option[PathOutput] = {
    val action = findOutputAction(path)
    
    runBlocking(action).flatMap(toPathOutput)
  }

  override def deleteOutput(paths: Iterable[Path]): Unit = {
    val delete = outputDeleteAction(paths)
    
    runBlocking(delete.transactionally)
  }
  
  private def insert(executionAndState: (Execution, JobState.CommandResult)): DBIO[Iterable[Int]] = {
    val (execution, commandResult) = executionAndState
    
    import Helpers.dummyId
    
    //NB: Note dummy ID, will be assigned an auto-increment ID by the DB :\
    val rawExecutionRow = new ExecutionRow(dummyId, commandResult.exitStatus)
    
    val pathBasedOutputs = execution.outputs.collect { case pb: Output.PathBased => pb }
    
    def toRawOutputRows(f: Output.PathBased => OutputRow): Seq[OutputRow] = {
      pathBasedOutputs.toSeq.map(f)
    }
    
    val outputs = {
      if(execution.isSuccess) { toRawOutputRows(pb => new OutputRow(pb.path, pb.hash)) }
      else if(execution.isFailure) { toRawOutputRows(pb => new OutputRow(pb.path)) }
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
  override def allOutputs: Seq[Output.PathBased] = {
    val query = tables.outputs.result
    
    log(query)
    
    runBlocking(query.transactionally).map(toOutput)
  }
  
  //TODO: Find way to extract common code from the all* methods
  override def allHashedOutputs: Seq[CachedOutput] = {
    val query = tables.outputs.filter { row =>
      row.hash.isDefined && row.hashType.isDefined && row.lastModified.isDefined
    }.result
    
    log(query)
    
    runBlocking(query.transactionally).map(_.toCachedOutput)
  }
  
  //TODO: Find way to extract common code from the all* methods
  override def allFailedOutputs: Seq[PathOutput] = {
    val query = tables.outputs.filter { row =>
      row.hash.isEmpty && row.hashType.isEmpty && row.lastModified.isEmpty
    }.result
    
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

  private def toOutput(row: OutputRow): Output.PathBased = row match {
    case OutputRow(_, Some(_), Some(_), Some(_), _) => row.toCachedOutput
    case _ => row.toPathOutput
  }
  
  private def toCachedOutput(row: OutputRow): Option[CachedOutput] = {
    Option(toOutput(row)).collect  { case co: CachedOutput => co }
  }
  
  private def toPathOutput(row: OutputRow): Option[PathOutput] = {
    Option(toOutput(row)).collect  { case po: PathOutput => po }
  }
  
  private object Implicits {
    //TODO: re-evaluate; does this make sense?
    implicit val dbExecutionContext: ExecutionContext = db.executor.executionContext
  }
  
  private def findOutputAction(path: Path): DBIO[Option[OutputRow]] = {
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
    val rawRows = rows.map(row => new OutputRow(row.path, row.hash))

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
    
    def outputByPath(path: Path) = { 
      val lookingFor = normalize(path)

      tables.outputs.filter(_.path === lookingFor).take(1)
    }
    
    def outputsByPaths(paths: Iterable[Path]) = {
      val rawPaths = paths.map(normalize).toSet
      
      outputsByRawPaths(rawPaths)
    }
    
    def outputsByRawPaths(rawPaths: Iterable[String]) = {
      tables.outputs.filter(_.path.inSetBind(rawPaths))
    }
  }
  
  private def reify(executionRow: ExecutionRow): Execution = {
    executionRow.toExecution(outputsFor(executionRow).toSet)
  }
  
  //TODO: There must be a better way than a subquery
  private def outputsFor(execution: ExecutionRow): Seq[Output] = {
    val query = tables.outputs.filter(_.executionId === execution.id).result
    
    runBlocking(query).map(toOutput)
  }
  
  //TODO: Re-evaluate; block all the time?
  private def runBlocking[A](action: DBIO[A]): A = waitFor(db.run(action))

  private def log(sqlAction: SqlAction[_, _, _]): Unit = {
    sqlAction.statements.foreach(s => debug(s"SQL: $s"))
  }
  
  private[slick] lazy val db = Database.forURL(descriptor.url, driver = descriptor.dbType.jdbcDriverClass)

  private[slick] lazy val tables = new Tables(driver)
}