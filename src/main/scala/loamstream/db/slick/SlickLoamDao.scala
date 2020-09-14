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
    LoamDao with CommonDaoOps with OutputDaoOps with ExecutionDaoOps with RunDaoOps with CommandDaoOps
