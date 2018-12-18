package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob

/**
 * @author clint
 * Nov 14, 2018
 */
final class JobFilterTest extends FunSuite {
  import JobFilterTest.MockJobFilter
  
  val alwaysSaysRun: JobFilter = new MockJobFilter(toReturn = true)
  val neverSaysRun: JobFilter = new MockJobFilter(toReturn = false)
  
  test("&&") {
    val t = alwaysSaysRun
    val f = neverSaysRun
    
    val job = MockJob(JobStatus.Succeeded)
    
    /*
     *  T && T => T
     *  T && F => F
     *  F && T => F
     *  F && F => F
     */
    assert((t && t).shouldRun(job) === true)
    assert((t && f).shouldRun(job) === false)
    assert((f && t).shouldRun(job) === false)
    assert((f && f).shouldRun(job) === false)
  }
  
  test("||") {
    val t = alwaysSaysRun
    val f = neverSaysRun
    
    val job = MockJob(JobStatus.Succeeded)
    
    /*
     *  T && T => T
     *  T && F => T
     *  F && T => T
     *  F && F => F
     */
    assert((t || t).shouldRun(job) === true)
    assert((t || f).shouldRun(job) === true)
    assert((f || t).shouldRun(job) === true)
    assert((f || f).shouldRun(job) === false)
  }
}

object JobFilterTest {
  private final class MockJobFilter(toReturn: Boolean) extends JobFilter {
    override def shouldRun(job: LJob): Boolean = toReturn
  }
}
