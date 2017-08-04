package loamstream.model.execute

import loamstream.db.LoamDao
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.{Execution, LJob, OutputRecord}
import loamstream.util.Loggable

/**
 * @author clint
 *         kyuksel
 * date: Sep 30, 2016
 */
final class DbBackedJobFilter(val dao: LoamDao) extends JobFilter with Loggable {
  override def shouldRun(job: LJob): Boolean = {
    def anyOutputNeedsToBeRun = job.outputs.exists(o => needsToBeRun(job.toString, o.toOutputRecord))

    val noOutputs = job.outputs.isEmpty
    val distinctCommand = hasDistinctCommand(job)

    if (noOutputs) { debug(s"Job $job will be run because it has no known outputs.") }
    if (distinctCommand) { debug(s"Job $job will be run because its command changed.") }

    noOutputs || anyOutputNeedsToBeRun || distinctCommand
  }

  override def record(executions: Iterable[Execution]): Unit = {
    //NB: We can only insert command executions (UGER or command-line jobs, anything with an in exit status code)
    //for now
    val insertableExecutions = executions.filter(_.isCommandExecution)

    debug(s"RECORDING $insertableExecutions")

    dao.insertExecutions(insertableExecutions)
  }

  // If performance becomes an issue, not 'findOutput()'ing multiple times
  // for a given OutputRecord should help
  private[execute] def needsToBeRun(jobStr: String, rec: OutputRecord): Boolean = {
    val msg = s"Job $jobStr will be run because its output"

    lazy val missing = rec.isMissing
    lazy val older = isOlder(rec)
    lazy val noHash = notHashed(rec)
    lazy val differentHash = hasDifferentHash(rec)

    if (missing) { debug(s"$msg $rec is missing.") }
    else if (older) { debug(s"$msg $rec is older.") }
    else if (noHash) { debug(s"$msg $rec does not have a hash value.") }
    else if (differentHash) { debug(s"$msg $rec has a different hash.") }

    missing || older || noHash || differentHash
  }

  private def findOutput(loc: String): Option[OutputRecord] = {
    dao.findOutputRecord(loc)
  }
  private[execute] def findCommand(loc: String): Option[String] = {
    dao.findCommand(loc)
  }

  private def isHashed(rec: OutputRecord): Boolean = {
    findOutput(rec.loc) match {
      case Some(matchingRec) => matchingRec.isHashed
      case None => false
    }
  }

  private def notHashed(rec: OutputRecord): Boolean = !isHashed(rec)

  private[execute] def hasDifferentHash(rec: OutputRecord): Boolean = {
    findOutput(rec.loc) match {
      case Some(matchingRec) => matchingRec.hasDifferentHashThan(rec)
      case None => false
    }
  }

  private[execute] def isOlder(currentRec: OutputRecord): Boolean = {
    findOutput(currentRec.loc) match {
      case Some(matchingRec) => currentRec.isOlderThan(matchingRec)
      case None => false
    }
  }

  private[execute] def hasDistinctCommand(job: LJob): Boolean = job match {
    case clj: CommandLineJob if job.outputs.nonEmpty =>
      !findCommand(clj.outputs.head.location).contains(clj.commandLineString)
    case _ => false
  }

  private[execute] def hasSameCommandIfAny(job: LJob): Boolean = !hasDistinctCommand(job)
}
