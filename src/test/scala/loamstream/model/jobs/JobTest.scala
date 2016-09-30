package loamstream.model.jobs

import org.scalatest.FunSuite

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  
  /*
   * 2
   *  \
   *   2+2 - (2+2)+1
   *  /
   * 2
   */
  test("isLeaf") {
    assert(two0.isLeaf)
    assert(two1.isLeaf)
    
    assert(twoPlusTwo.isLeaf === false)
    assert(plusOne.isLeaf === false)
  }
  
  /*
   * 2
   *  \
   *   2+2 - (2+2)+1
   *  /
   * 2
   */
  test("leaves()") {
    assert(two0.leaves == Set(two0))
    assert(two1.leaves == Set(two1))
    
    assert(twoPlusTwo.leaves == Set(two0, two1))
    
    assert(plusOne.leaves == Set(two0, two1))
  }

  test("isRunnable - single job") {
    val singleJob  = MockJob(JobState.Succeeded, "single job", Set.empty, Set.empty, 0)

    assert(singleJob.state == JobState.NotStarted)
    
    assert(singleJob.isRunnable)
  }
  
  test("isRunnable - linear 2 job pipeline") {
    val dep = MockJob(JobState.Succeeded, "dep", Set.empty, Set.empty, 0)
    
    val root = MockJob(JobState.Succeeded, "root", Set(dep), Set.empty, 0)

    assert(dep.state == JobState.NotStarted)
    assert(root.state == JobState.NotStarted)
    
    assert(dep.isRunnable)
    assert(root.isRunnable == false)
  }
}