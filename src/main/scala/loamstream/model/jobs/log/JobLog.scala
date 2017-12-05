package loamstream.model.jobs.log

import loamstream.util.Loggable
import loamstream.model.jobs.JobRun
import loamstream.model.jobs.JobStatus
import loamstream.util.Functions

/**
 * @author clint
 * Sep 28, 2017
 */
object JobLog extends Loggable {
  private def numCharsFor(js: JobStatus): Int = js.toString.length + 2
  
  private val maxStatusChars = JobStatus.values.map(numCharsFor).max
  
  private[jobs] val computePadding: JobStatus => String = Functions.memoize { status => 
    val numPaddingChars = maxStatusChars - numCharsFor(status)
      
    " " * numPaddingChars
  }
  
  def onStatusChange(jobRun: JobRun): Unit = {
    val JobRun(job, status, runCount) = jobRun
    
    val padding: String = computePadding(status)
    
    info(s"($runCount runs) <$status>$padding - $job")
  }
}
