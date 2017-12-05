package loamstream.model.jobs

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 14, 2017
 */
final class JobRunTest extends FunSuite {

  import JobStatus._
  
  test("toString") {
    val job = RxMockJob("foo")
    
    val run = JobRun(job, Submitted, 42)
    
    assert(run.toString === "JobRun(foo,Submitted,42)")
  }
  
  test("equals/hashCode") {
    val job0 = RxMockJob("foo")
    val job1 = RxMockJob("bar")
    val job2 = RxMockJob("baz")
    
    assert(job0 != job1)
    assert((job0 eq job1) === false)
    assert(job0.hashCode != job1.hashCode)
    
    assert(JobRun(job0, Running, 1) != JobRun(job1, Running, 1))
    assert(JobRun(job1, Running, 1) != JobRun(job0, Running, 1))
    assert(JobRun(job1, Running, 1).hashCode != JobRun(job0, Running, 1).hashCode)
    
    assert(JobRun(job1, Running, 1) == JobRun(job1, Running, 1))
    assert(JobRun(job0, Running, 1) == JobRun(job0, Running, 1))
    assert(JobRun(job1, Running, 1).hashCode == JobRun(job1, Running, 1).hashCode)
    assert(JobRun(job0, Running, 1).hashCode == JobRun(job0, Running, 1).hashCode)
    
    //different run count
    assert(JobRun(job0, Skipped, 0) != JobRun(job0, Skipped, 42))
    assert(JobRun(job0, Skipped, 0).hashCode != JobRun(job0, Skipped, 42).hashCode)
    //different status
    assert(JobRun(job0, Skipped, 0) != JobRun(job0, NotStarted, 0))
    assert(JobRun(job0, Skipped, 0).hashCode != JobRun(job0, NotStarted, 0).hashCode)
    //different job
    assert(JobRun(job0, Skipped, 0) != JobRun(job1, Skipped, 0))
    assert(JobRun(job0, Skipped, 0).hashCode != JobRun(job1, Skipped, 0).hashCode)
    
    assert(JobRun(job0, Skipped, 0) === JobRun(job0, Skipped, 0))
    assert(JobRun(job0, Skipped, 0).hashCode === JobRun(job0, Skipped, 0).hashCode)
    
    val run0 = JobRun(job0, job0.status, job0.runCount)
    val run1 = JobRun(job0, run0.status, run0.runCount)
    
    assert((run0 eq run1) === false)
    assert(run0 == run1)
    assert(run0.hashCode == run1.hashCode)
    
    assert(job0.status === NotStarted)
    assert(job0.runCount === 0)
    
    //Give job0 a different run count and status 
    job0.transitionTo(Running)
    
    assert(job0.status === Running)
    assert(job0.runCount === 1)
    
    assert(run0 == run1)
    assert(run0.hashCode == run1.hashCode)
  }
  
  test("unapply") {
    val job = RxMockJob("foo")
    
    val run = JobRun(job, Submitted, 42)
    
    assert(JobRun.unapply(run) === Some((job, Submitted, 42)))
  }
}
