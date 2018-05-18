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
final class BjobsPoller private[lsf] (pollingFn: BjobsPoller.PollingFn) extends Poller with Loggable {
  /**
   * Synchronously inquire about the status of one or more jobs
   *
   * @param jobIds the ids of the jobs to inquire about
   * @return a map of job ids to attempts at that job's status
   */
  override def poll(jobIds: Iterable[String]): Map[String, Try[DrmStatus]] = {
    debug(s"Polling for ${jobIds.size} jobs: $jobIds")
    
    val allLsfJobIdsAttempt = Tries.sequence(jobIds.map(LsfJobId.parse)) 
    
    //TODO: .get
    val allLsfJobIds = allLsfJobIdsAttempt.get
    
    import Maps.Implicits._
    
    val indicesByBaseId: Map[String, Set[LsfJobId]] = allLsfJobIds.toSet.groupBy(_.baseJobId)
    
    val jobIdsToStatusAttempts = Maps.mergeMaps(indicesByBaseId.values.map(runChunk))
    
    val result = jobIdsToStatusAttempts.mapKeys(_.asString)
    
    debug(s"Done polling for $jobIds; results: $result")
    
    result
  }
  
  private[lsf] def runChunk(lsfJobIds: Set[LsfJobId]): Map[LsfJobId, Try[DrmStatus]] = {
    val runResultsAttempt = pollingFn(lsfJobIds)
      
    val chunkOfIdsToStatusesAttempt = runResultsAttempt.flatMap { runResults =>
      if(runResults.isFailure) { 
        import runResults.exitCode
        
        runResults.logStdOutAndStdErr(s"LSF polling failure (exit code ${exitCode}), stdout and stderr follow:")
        
        Tries.failure(s"Error polling for job ids: $lsfJobIds")
      } else {
        Try(BjobsPoller.parseBjobsOutput(runResults.stdout).toMap)
      }
    }
    
    chunkOfIdsToStatusesAttempt match { 
      case Failure(e) => {
        error(s"Error polling for LSF job ids ${lsfJobIds.map(_.asString)} : ${e.getMessage}", e)
    
        import loamstream.util.Traversables.Implicits._
        
        lsfJobIds.mapTo(_ => Failure(e))
      }
      case Success(idsToStatuses) => {
        import loamstream.util.Maps.Implicits._
        
        idsToStatuses.strictMapValues(Success(_))
      }
    }
  }
  
  override def stop(): Unit = ()
}

object BjobsPoller extends Loggable {
  
  type PollingFn = Set[LsfJobId] => Try[RunResults]
  
  def fromExecutable(actualExecutable: String = "bjobs"): BjobsPoller = {
    def pollingFn(lsfJobIds: Set[LsfJobId]): Try[RunResults] = {
      val tokens = makeTokens(actualExecutable, lsfJobIds)
      
      import scala.sys.process._
      
      debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      //NB: Implicit conversion to ProcessBuilder :\
      val processBuilder: ProcessBuilder = tokens
      
      Processes.runSync(actualExecutable, processBuilder)
    }
    
    new BjobsPoller(pollingFn)
  }
  
  private[lsf] def parseBjobsOutput(lines: Seq[String]): Iterable[(LsfJobId, DrmStatus)] = {
    val dataLines = lines.map(_.trim).filter(_.nonEmpty)
    
    //NB: Bjobs output is expected to look like this (minus the header line)
    //JOBID     JOB_NAME      STAT  EXIT_CODE
    //2842408   LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1] EXIT  42        ",
    //2842408   LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3] DONE      -     ",
    //2842408   LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ")
    
    //TODO: Unparseable lines are ignored; revisit whether this is appropriate
    dataLines.flatMap(parseBjobsOutputLine)
  }
  
  private val jobNameArrayIndexRegex: Regex = """^.+?\[(\d+)\]""".r
  
  private[lsf] def parseBjobsOutputLine(line: String): Option[(LsfJobId, DrmStatus)] = {
    trace(s"parsing bjobs output line '$line'")
    
    //NB: Bjobs output is expected to look like this (minus the header line)
    //JOBID     JOB_NAME      STAT  EXIT_CODE
    //2842408   LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1] EXIT  42        ",
    //2842408   LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3] DONE      -     ",
    //2842408   LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2] DONE      -     ")
    val parts = line.split("""\s+""")
    
    def extractIndex(s: String): Option[Int] = s.trim match {
      case jobNameArrayIndexRegex(i) => Some(i.toInt)
      case _ => None
    }
    
    val liftedParts = parts.lift
    
    val result = for {
      baseJobId <- liftedParts(0)
      jobNameWithIndex <- liftedParts(1)
      taskArrayIndex <- extractIndex(jobNameWithIndex)
      statusString <- liftedParts(2)
      status <- LsfStatus.fromString(statusString)
      exitCodeString <- liftedParts(3)
    } yield {
      val exitCodeOpt = Try(exitCodeString.trim.toInt).toOption
      val drmStatus = status.toDrmStatus(exitCodeOpt)
      
      LsfJobId(baseJobId, taskArrayIndex) -> drmStatus
    }
    
    if(result.isEmpty) {
      warn(s"Couldn't parse bjobs output line '$line'")
    }
    
    result
  }
  
  private[lsf] def makeTokens(actualExecutable: String, lsfJobIds: Iterable[LsfJobId]): Seq[String] = {
    require(lsfJobIds.nonEmpty, s"Can't build '${actualExecutable}' command-line from empty set of job ids")
    
    val baseJobIds = lsfJobIds.map(_.baseJobId).toSet
    
    require(baseJobIds.size == 1, s"All LSF job ids in this chunk should have the same base, but got $lsfJobIds")
    
    val indices = lsfJobIds.map(_.taskArrayIndex)
    
    val Seq(baseJobId) = baseJobIds.toSeq
        
    val toQueryFor = s"${baseJobId}[${indices.mkString(",")}]"
    
    //NB: See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.2/lsf_command_ref/bjobs.1.html
    Seq(
        actualExecutable, 
        "-noheader", //Don't print a header row
        "-d",        //"Displays information about jobs that finished recently"
        "-r",        //Displays running jobs
        "-s",        //Display suspended jobs
        "-o",        //Specify output columns
        "jobid: job_name:-100 stat: exit_code:", 
        toQueryFor)
  }
}
