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
import loamstream.model.jobs.StoreRecord
import loamstream.util.Loggable

/**
 * @author clint
 *         kyuksel
 *         date: 8/8/2016
 *
 * LoamDao implementation backed by Slick
 * For a schema description, see Tables
 */
final class SlickLoamDao(val descriptor: DbDescriptor) extends 
    LoamDao with CommonDaoOps with OutputDaoOps with ExecutionDaoOps {
  
  override val driver = descriptor.dbType.driver

  import driver.api._

  protected[slick] override lazy val db: driver.backend.DatabaseDef = {
    Database.forURL(url = descriptor.url, driver = descriptor.dbType.jdbcDriverClass)
  }

  protected[slick] override lazy val tables: Tables = new Tables(driver)
  
  override def findCommand(loc: String): Option[String] = {
    for {
      outputRow <- findOutputRow(loc)
      executionId <- outputRow.executionId
      executionRow <- findExecutionRow(executionId)
      commandLine <- executionRow.cmd
    } yield commandLine
  }

  override def createTables(): Unit = tables.create(db)

  override def dropTables(): Unit = tables.drop(db)

  override def shutdown(): Unit = blockOn(db.shutdown)
}
