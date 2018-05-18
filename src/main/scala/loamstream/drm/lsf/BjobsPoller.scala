package loamstream.drm.lsf

import loamstream.drm.Poller
import scala.util.Try
import loamstream.drm.DrmStatus
import scala.util.matching.Regex
import loamstream.util.Tries
import loamstream.util.Maps
import loamstream.util.Loggable
import scala.util.Failure
import scala.util.Success

/**
 * @author clint
 * May 15, 2018
 */
final class BjobsPoller(actualExecutable: String = "bjobs") extends Poller with Loggable {
  /**
   * Synchronously inquire about the status of one or more jobs
   *
   * @param jobIds the ids of the jobs to inquire about
   * @return a map of job ids to attempts at that job's status
   */
  override def poll(jobIds: Iterable[String]): Map[String, Try[DrmStatus]] = {
    val allLsfJobIdsAttempt = Tries.sequence(jobIds.map(LsfJobId.parse)) 
    
    //TODO: .get
    val allLsfJobIds = allLsfJobIdsAttempt.get
    
    import Maps.Implicits._
    
    val indicesByBaseId: Map[String, Set[LsfJobId]] = allLsfJobIds.toSet.groupBy(_.baseJobId)
    
    val jobIdsToStatusAttempts = Maps.mergeMaps(indicesByBaseId.values.map(runChunk))
    
    jobIdsToStatusAttempts.map { case (lsfJobId, statusAttempt) => 
      lsfJobId.asString -> statusAttempt.map(_.toDrmStatus)
    }
  }
  
  private def runChunk(lsfJobIds: Set[LsfJobId]): Map[LsfJobId, Try[LsfStatus]] = {
    val tokens = BjobsPoller.makeTokens(actualExecutable, lsfJobIds)
      
    import scala.sys.process._
    import loamstream.util.Maps.Implicits._
    
    //NB: Implicit conversion to ProcessBuilder :\
    val processBuilder: ProcessBuilder = tokens
    
    val runResultsAttempt = Processes.runSync(actualExecutable, processBuilder)
    
    val chunkOfIdsToStatusesAttempt = for {
      runResults <- runResultsAttempt
    } yield {
      BjobsPoller.parseBjobsOutput(runResults.stdout).toMap
    }
    
    chunkOfIdsToStatusesAttempt match { 
      case Failure(e) => {
        error(s"Error polling for LSF job ids ${lsfJobIds.map(_.asString)} : ${e.getMessage}", e)
    
        import loamstream.util.Traversables.Implicits._
        
        lsfJobIds.mapTo(_ => Failure(e))
      }
      case Success(idsToStatuses) => idsToStatuses.strictMapValues(Success(_))
    }
  }
  
  override def stop(): Unit = ()
  
  /**
   * JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
		 2738574 cgilber DONE  research-r ebi-cli-002 ebi6-062    *oworld[2] May 16 23:19
   */
}

object BjobsPoller {
  
  
  private[lsf] def parseBjobsOutput(lines: Seq[String]): Iterable[(LsfJobId, LsfStatus)] = {
    val trimmedNonEmptyLines = lines.map(_.trim).filter(_.nonEmpty)
    
    val dataLines = trimmedNonEmptyLines.dropWhile(_.startsWith("JOBID"))
    
    /*
      JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
      2842408 cgilbert DONE  research-rh7 ebi-cli-002 ebi5-270    helloworld[2] May 17 20:14
      2842408 cgilbert DONE  research-rh7 ebi-cli-002 ebi6-116    helloworld[3] May 17 20:14
      2842408 cgilbert DONE  research-rh7 ebi-cli-002 hx-noah-10-09 helloworld[1] May 17 20:14
     */
    
    dataLines.flatMap(parseBjobsOutputLine)
  }
  
  private val jobNameArrayIndexRegex: Regex = """^\w+?\[(\d+)\]""".r
  
  private[lsf] def parseBjobsOutputLine(line: String): Option[(LsfJobId, LsfStatus)] = {
    //NB: note use of take(7) to drop the SUBMIT_TIME column.  This is needed since values in that column
    //contain spaces, and columns are delimited by one or more spaces, not tabs. :\  For example:
    //
    //JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
    //2842408 cgilbert DONE  research-rh7 ebi-cli-002 ebi5-270    helloworld[2] May 17 20:14
    val parts = line.split("""\s+""").take(7)
    
    def extractIndex(s: String): Option[Int] = s match {
      case jobNameArrayIndexRegex(i) => Some(i.toInt)
      case _ => None
    }
    
    val liftedParts = parts.lift
    
    for {
      baseJobId <- liftedParts(0)
      statusString <- liftedParts(2)
      status <- LsfStatus.fromString(statusString)
      jobNameWithIndex <- liftedParts(6)
      taskArrayIndex <- extractIndex(jobNameWithIndex)
    } yield {
      LsfJobId(baseJobId, taskArrayIndex) -> status
    }
  }
  
  private[lsf] def makeTokens(actualExecutable: String, lsfJobIds: Iterable[LsfJobId]): Seq[String] = {
    require(lsfJobIds.nonEmpty, s"Can't build '${actualExecutable}' command-line from empty set of job ids")
    
    val baseJobIds = lsfJobIds.map(_.baseJobId).toSet
    
    require(baseJobIds.size == 1, s"All lsf job ids should have the same base, but got $lsfJobIds")
    
    val indices = lsfJobIds.map(_.taskArrayIndex)
    
    val Seq(baseJobId) = baseJobIds.toSeq
        
    val toQueryFor = s"${baseJobId}[${indices.mkString(",")}]"
    
    //NB: -w means "don't truncate any values"
    Seq(actualExecutable, "-w", toQueryFor)
  }
}
