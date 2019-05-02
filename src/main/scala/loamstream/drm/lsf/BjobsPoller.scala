package loamstream.drm.lsf

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

import InvokesBjobs.InvocationFn
import loamstream.drm.DrmStatus
import loamstream.drm.Poller
import loamstream.model.quantities.CpuTime
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.Options
import loamstream.util.Tries

/**
 * @author clint
 * May 15, 2018
 */
final class BjobsPoller private[lsf] (pollingFn: InvocationFn[Set[LsfJobId]]) extends Poller with Loggable { 
  
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
    
    import loamstream.util.Maps.Implicits._
    
    val indicesByBaseId: Map[String, Set[LsfJobId]] = allLsfJobIds.toSet.groupBy(_.baseJobId)
    
    val jobIdsToStatusAttempts = Maps.mergeMaps(indicesByBaseId.values.map(runChunk))
    
    val result = jobIdsToStatusAttempts.mapKeys(_.asString)
    
    debug(s"Done polling for $jobIds; results: $result")
    
    result
  }
  
  override def stop(): Unit = ()
  
  private[lsf] def runChunk(lsfJobIds: Set[LsfJobId]): Map[LsfJobId, Try[DrmStatus]] = {
    val runResultsAttempt = pollingFn(lsfJobIds)
      
    val chunkOfIdsToStatusesAttempt = runResultsAttempt.flatMap { runResults =>
      Try(BjobsPoller.parseBjobsOutput(runResults.stdout).toMap)
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
}

object BjobsPoller extends InvokesBjobs.Companion(new BjobsPoller(_)) {
  
  private[lsf] def parseBjobsOutput(lines: Seq[String]): Iterable[(LsfJobId, DrmStatus)] = {
    val dataLines = lines.map(_.trim).filter(_.nonEmpty)
    
    // scalastyle:off line.size.limit
    //
    // NB: Bjobs output is expected to look like this (minus the header line)
    // JOBID  |  JOB_NAME                                          |STAT |EXIT_CODE
    // 3980940|  LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1]|EXIT |42       
    // 3980940|  LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2]|DONE |   -     
    // 3980940|  LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3]|DONE |   -     
    // 
    // scalastyle:on line.size.limit
    
    //TODO: Unparseable lines are ignored; revisit whether this is appropriate
    dataLines.flatMap(parseBjobsOutputLine(_).toOption)
  }
  
  private val jobNameArrayIndexRegex: Regex = """^.+?\[(\d+)\]""".r
  
  private[lsf] def parseCpuTime(lsfRep: String): Try[CpuTime] = {
    //lsfRep will look like'42.123 seconds' or '0.0 second', minus quotes;
    //Just drop the units and assume seconds (they're the default unit)
    Try {
      val Array(amount, _) = lsfRep.trim.split("""\s+""")
      
      CpuTime.inSeconds(amount.toDouble)
    }
  }
  
  private[lsf] def parseBjobsOutputLine(line: String): Try[(LsfJobId, DrmStatus)] = {
    trace(s"parsing bjobs output line '$line'")
    
    // scalastyle:off line.size.limit
    //
    // NB: Bjobs output is expected to look like this (minus the header line)
    // JOBID  |  JOB_NAME                                          |STAT |EXIT_CODE 
    // 3980940|  LoamStream-826b3929-4810-4116-8502-5c60cd830d81[1]|EXIT |42        
    // 3980940|  LoamStream-826b3929-4810-4116-8502-5c60cd830d81[2]|DONE |   -      
    // 3980940|  LoamStream-826b3929-4810-4116-8502-5c60cd830d81[3]|DONE |   -      
    // 
    // scalastyle:on line.size.limit
    val parts = line.split("\\|").map(_.trim)
    
    def extractIndex(s: String): Try[Int] = s match {
      case jobNameArrayIndexRegex(i) => Try(i.toInt)
      case _ => Tries.failure(s"Couldn't extract job array index from '${s}'")
    }
    
    val liftedParts: Int => Try[String] = { index =>
      if(parts.isDefinedAt(index)) {
        Success(parts(index))
      } else {
        Tries.failure(s"Index $index is out of bounds; parts: ${parts.mkString("Array(", ",", ")")}")
      }
    }
    
    val resultAttempt = for {
      baseJobId <- liftedParts(0)
      jobNameWithIndex <- liftedParts(1)
      taskArrayIndex <- extractIndex(jobNameWithIndex)
      statusString <- liftedParts(2)
      status <- Options.toTry(LsfStatus.fromString(statusString))(s"Couldn't determine LSF status for '$statusString'")
      exitCodeString <- liftedParts(3)
    } yield {
      val exitCodeOpt = Try(exitCodeString.toInt).toOption
      
      val drmStatus = status.toDrmStatus(exitCodeOpt)
      
      LsfJobId(baseJobId, taskArrayIndex) -> drmStatus
    }
    
    if(resultAttempt.isFailure) {
      warn(s"Couldn't parse bjobs output line '$line'")
    }
    
    resultAttempt
  }
  
  private[lsf] override def makeTokens(actualExecutable: String, lsfJobIds: Set[LsfJobId]): Seq[String] = {
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
        // scalastyle:off line.size.limit
        "jobid: job_name:-75 stat: exit_code: delimiter='|'",
        // scalastyle:on line.size.limit
        toQueryFor)
  }
}
