package loamstream.drm

import org.scalatest.FunSuite

import scala.collection.compat._

/**
 * @author clint
 * Jan 12, 2021
 */
final class SessionTrackerTest extends FunSuite {
  private val tid0 = DrmTaskId("foo", 1)
  private val tid1 = DrmTaskId("foo", 42)
  private val tid2 = DrmTaskId("bar", 11)
  private val tid3 = DrmTaskId("baz", 2)
  private val tid4 = DrmTaskId("blerg", 2)
  
  val allTids = Seq(tid0, tid1, tid2, tid3, tid4)
  
  test("Default") {
    val tracker = SessionTracker.Default.empty
    
    assert(tracker.isEmpty === true)
    assert(tracker.nonEmpty === false)
    assert(tracker.taskArrayIdsSoFar.isEmpty)
    
    tracker.register(Seq(tid1, tid0))
    
    assert(tracker.isEmpty === false)
    assert(tracker.nonEmpty === true)
    assert(tracker.taskArrayIdsSoFar.to(Set) === Set("foo"))
    
    tracker.register(Nil)
    
    assert(tracker.isEmpty === false)
    assert(tracker.nonEmpty === true)
    assert(tracker.taskArrayIdsSoFar.to(Set) === Set("foo"))
    
    tracker.register(Seq(tid3, tid4, tid2))
    
    assert(tracker.isEmpty === false)
    assert(tracker.nonEmpty === true)
    assert(tracker.taskArrayIdsSoFar.to(Set) === Set("foo", "bar", "baz", "blerg"))
  }
  
  test("Noop") {
    val tracker = SessionTracker.Noop
    
    assert(tracker.isEmpty === true)
    assert(tracker.nonEmpty === false)
    assert(tracker.taskArrayIdsSoFar.isEmpty)
    
    tracker.register(Seq(tid1, tid0))
    
    assert(tracker.isEmpty === true)
    assert(tracker.nonEmpty === false)
    assert(tracker.taskArrayIdsSoFar.isEmpty)
    
    tracker.register(Nil)
    
    assert(tracker.isEmpty === true)
    assert(tracker.nonEmpty === false)
    assert(tracker.taskArrayIdsSoFar.isEmpty)
    
    tracker.register(Seq(tid3, tid4, tid2))
    
    assert(tracker.isEmpty === true)
    assert(tracker.nonEmpty === false)
    assert(tracker.taskArrayIdsSoFar.isEmpty)
  }
}
