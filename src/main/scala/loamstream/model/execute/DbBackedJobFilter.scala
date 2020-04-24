package loamstream.model.execute

import loamstream.db.LoamDao
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.{Execution, LJob, StoreRecord}
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
    val lastFailure = lastFailureStatus(job)
    val failed = lastFailure.isDefined 
    lazy val distinctCommand = hasNewCommandLine(job)
    lazy val outputThatCausesRunning = outputThatCausesRunningIfAny(job)
    lazy val anyOutputNeedsToBeRun = outputThatCausesRunning.isDefined

    def msg = s"Job $job will be run because"
    
    if (failed) { trace(s"$msg it failed last time with status: ${lastFailure.get}") }
    else if (distinctCommand) { trace(s"$msg its command changed") }
    else if (anyOutputNeedsToBeRun) { trace(s"$msg of this output: ${outputThatCausesRunning.get.location}") }

    failed || distinctCommand || anyOutputNeedsToBeRun
  }

  // If performance becomes an issue, not 'findOutput()'ing multiple times
  // for a given OutputRecord should help
  private[execute] def needsToBeRun(jobStringRep: String, output: StoreRecord): Boolean = {
    val msg = s"Job $jobStringRep will be run because its output"

    val considerHashes = outputHashingStrategy.shouldHash
    
    val missingFromDisk = output.isMissing
    lazy val differentModTime = hasDifferentModTime(output)
    lazy val noHashInDb = notHashedInDb(output)
    lazy val differentHashInDb = hasDifferentHash(output)
    lazy val absentOrDifferentHash = (considerHashes && (noHashInDb || differentHashInDb))
    
    if (missingFromDisk) { trace(s"$msg $output is missing.") }
    else if (differentModTime) { trace(s"$msg $output is older.") }
    else if(considerHashes) {
      if (noHashInDb) { trace(s"$msg $output does not have a hash value.") }
      else if (differentHashInDb) { trace(s"$msg $output has a different hash.") }
    }

    missingFromDisk || differentModTime || absentOrDifferentHash
  }

  private def findOutputInDb(loc: String): Option[StoreRecord] = dao.findStoreRecord(loc)

  private[execute] def findCommandLineInDb(loc: String): Option[String] = dao.findCommand(loc)

  private def isHashedInDb(onDisk: StoreRecord): Boolean = {
    compareWithDb(onDisk)(fromDb => fromDb.isHashed)
  }

  private def notHashedInDb(onDisk: StoreRecord): Boolean = !isHashedInDb(onDisk)

  private[execute] def hasDifferentHash(onDisk: StoreRecord): Boolean = {
    compareWithDb(onDisk)(fromDb => onDisk.hasDifferentHashThan(fromDb))
  }

  private[execute] def hasDifferentModTime(onDisk: StoreRecord): Boolean = {
    compareWithDb(onDisk)(fromDb => onDisk.hasDifferentModTimeThan(fromDb))
  }
  
  private def compareWithDb(onDisk: StoreRecord)(compare: StoreRecord => Boolean): Boolean = {
    findOutputInDb(onDisk.loc) match {
      case Some(fromDb) => compare(fromDb)
      case None => false
    }
  }

  private[execute] def hasNewCommandLine(job: LJob): Boolean = job match {
    case CommandLineJob.WithCommandLineAndOutputs(newCommandLine, outputs) if outputs.nonEmpty => {
      val recordedCommandLine = findCommandLineInDb(outputs.head.location)
      
      recordedCommandLine != Some(newCommandLine)
    }
    case _ => false
  }
  
  private[execute] def lastFailureStatus(job: LJob): Option[JobStatus] = {
    job.outputs.headOption.map(_.location).flatMap(dao.findLastStatus).filter(_.isFailure)
  }
  
  private[execute] def outputThatCausesRunningIfAny(job: LJob): Option[DataHandle] = {
    job.outputs.collectFirst { case o if needsToBeRun(job.toString, o.toStoreRecord) => o }
  }
}
