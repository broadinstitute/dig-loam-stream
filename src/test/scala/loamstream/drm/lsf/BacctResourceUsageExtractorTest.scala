package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.quantities.Memory
import scala.util.Success
import java.time.Instant
import loamstream.drm.Queue
import loamstream.model.quantities.CpuTime
import java.time.ZoneId
import java.time.temporal.ChronoField
import java.time.ZonedDateTime

/**
 * @author clint
 * Apr 18, 2019
 */
final class BacctResourceUsageExtractorTest extends FunSuite {
  test("Parse actual bacct outpout - bad input") {
    assert(BacctResourceUsageExtractor.toResources(Nil).isFailure === true)
    assert(BacctResourceUsageExtractor.toResources(Seq("x", "y", "z")).isFailure === true)
  }
  
  test("Parse actual bacct outpout - happy path") {
    val actual = BacctResourceUsageExtractor.toResources(actualOutput.split("\\n"))
    
    val now = ZonedDateTime.now
    
    val systemTimeZoneOffSet = now.getOffset.getId
    
    val currentYear: Int = now.get(ChronoField.YEAR)
    
    val expected = LsfResources(
            Memory.inMb(0), 
            CpuTime.inSeconds(0.02), 
            Option("ebi6-054"),
            Option(Queue("research-rh7")),
            ZonedDateTime.parse(s"${currentYear}-04-18T22:32:01.00${systemTimeZoneOffSet}").toInstant,
            ZonedDateTime.parse(s"${currentYear}-04-18T23:34:12.00${systemTimeZoneOffSet}").toInstant)
    
    assert(actual.get === expected)
  }
  
  private val actualOutput = """
    Accounting information about jobs that are: 
  - submitted by all users.
  - accounted on all projects.
  - completed normally or exited
  - executed on all hosts.
  - submitted to all queues.
  - accounted on all service classes.
------------------------------------------------------------------------------

Job <2811237>, User <cgilbert>, Project <default>, Status <EXIT>, Queue <research-rh7>, Command <cp ./A.txt ./B.txt>, Share group charged </cgilbert>
Thu Apr 18 22:32:01: Submitted from host <ebi-cli-001>, CWD <$HOME>;
Thu Apr 18 22:32:01: Dispatched to <ebi6-054>, Effective RES_REQ <select[type == local] order[r15s:pg] rusage[numcpus=1.00:duration=8h:decay=0] span[hosts=1] >;
Thu Apr 18 23:34:12: Completed <exit>.

Accounting information about this job:
     Share group charged </cgilbert>
     CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
      0.02        0              0     exit         0.0000     0M      0M
------------------------------------------------------------------------------

SUMMARY:      ( time unit: second ) 
 Total number of done jobs:       0      Total number of exited jobs:     1
 Total CPU time consumed:       0.0      Average CPU time consumed:     0.0
 Maximum CPU time of a job:     0.0      Minimum CPU time of a job:     0.0
 Total wait time in queues:     0.0
 Average wait time in queue:    0.0
 Maximum wait time in queue:    0.0      Minimum wait time in queue:    0.0
 Average turnaround time:         0 (seconds/job)
 Maximum turnaround time:         0      Minimum turnaround time:         0
 Average hog factor of a job:  0.00 ( cpu time / turnaround time )
 Maximum hog factor of a job:  0.00      Minimum hog factor of a job:  0.00
 Average expansion factor of a job:  0.00 ( turnaround time / run time )
 Maximum expansion factor of a job:  0.00
 Minimum expansion factor of a job:  0.00
 Total Run time consumed:         0      Average Run time consumed:       0
    """
}
