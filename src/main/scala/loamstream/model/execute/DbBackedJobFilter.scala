package loamstream.model.execute

import loamstream.db.LoamDao
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.{Execution, LJob, OutputRecord}
import loamstream.util.Loggable
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.DataHandle

/**
 * @author clint
 *         kyuksel
 * date: Sep 30, 2016
 */
final class DbBackedJobFilter(
    val dao: LoamDao, 
    val outputHashingStrategy: HashingStrategy = HashingStrategy.HashOutputs) extends JobFilter with Loggable {
  
  override def shouldRun(job: LJob): Boolean = {
    val noOutputs = job.outputs.isEmpty
    lazy val lastFailure = lastFailureStatus(job)
    lazy val failed = lastFailure.isDefined 
    lazy val distinctCommand = hasNewCommandLine(job)
    lazy val outputThatCausesRunning = outputThatCausesRunningIfAny(job)
    lazy val anyOutputNeedsToBeRun = outputThatCausesRunning.isDefined

    def msg = s"Job $job will be run because"
    
    if (noOutputs) { debug(s"$msg it has no known outputs") }
    else if (failed) { debug(s"$msg it failed last time with status: ${lastFailure.get}") }
    else if (distinctCommand) { debug(s"$msg its command changed") }
    else if (anyOutputNeedsToBeRun) { debug(s"$msg of this output: ${outputThatCausesRunning.get.location}") }

    noOutputs || failed || distinctCommand || anyOutputNeedsToBeRun
  }

  // If performance becomes an issue, not 'findOutput()'ing multiple times
  // for a given OutputRecord should help
  private[execute] def needsToBeRun(jobStr: String, rec: OutputRecord): Boolean = {
    val msg = s"Job $jobStr will be run because its output"

    val considerHashes = outputHashingStrategy.shouldHash
    
    lazy val missingFromDisk = rec.isMissing
    lazy val differentModTime = hasDifferentModTime(rec)
    lazy val noHashInDb = notHashedInDb(rec)
    lazy val differentHashInDb = hasDifferentHash(rec)
    lazy val absentOrDifferentHash = (considerHashes && (noHashInDb || differentHashInDb))
    
    if (missingFromDisk) { debug(s"$msg $rec is missing.") }
    else if (differentModTime) { debug(s"$msg $rec is older.") }
    else if(considerHashes) {
      if (noHashInDb) { debug(s"$msg $rec does not have a hash value.") }
      else if (differentHashInDb) { debug(s"$msg $rec has a different hash.") }
    }

    missingFromDisk || differentModTime || absentOrDifferentHash
  }

  private def findOutputInDb(loc: String): Option[OutputRecord] = dao.findOutputRecord(loc)

  private[execute] def findCommandLineInDb(loc: String): Option[String] = dao.findCommand(loc)

  private def isHashedInDb(onDisk: OutputRecord): Boolean = {
    compareWithDb(onDisk)(fromDb => fromDb.isHashed)
  }

  private def notHashedInDb(onDisk: OutputRecord): Boolean = !isHashedInDb(onDisk)

  private[execute] def hasDifferentHash(onDisk: OutputRecord): Boolean = {
    compareWithDb(onDisk)(fromDb => onDisk.hasDifferentHashThan(fromDb))
  }

  private[execute] def hasDifferentModTime(onDisk: OutputRecord): Boolean = {
    compareWithDb(onDisk)(fromDb => onDisk.hasDifferentModTimeThan(fromDb))
  }
  
  private def compareWithDb(onDisk: OutputRecord)(compare: OutputRecord => Boolean): Boolean = {
    findOutputInDb(onDisk.loc) match {
      case Some(fromDb) => compare(fromDb)
      case None => false
    }
  }

  private[execute] def hasNewCommandLine(job: LJob): Boolean = job match {
    case CommandLineJob(newCommandLine, outputs) if outputs.nonEmpty => {
      val recordedCommandLine = findCommandLineInDb(outputs.head.location)
      
      recordedCommandLine != Some(newCommandLine)
    }
    case _ => false
  }
  
  private[execute] def lastFailureStatus(job: LJob): Option[JobStatus] = {
    dao.findExecution(job.outputs.head.location).collect {
      case e if e.status.isFailure => e.status
    }
  }
  
  private[execute] def outputThatCausesRunningIfAny(job: LJob): Option[DataHandle] = {
    job.outputs.collectFirst { case o if needsToBeRun(job.toString, o.toOutputRecord) => o }
  }
}
