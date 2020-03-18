package loamstream.util

import org.scalatest.FunSuite
import scala.util.Try
import scala.util.Success
import scala.concurrent.duration._
import loamstream.TestHelpers
import rx.lang.scala.Observable


/**
 * @author clint
 * May 2, 2019
 */
final class LoopsTest extends FunSuite {
  private def alwaysWorks: Try[Int] = Success(42)
  private def alwaysFails: Try[Int] = Tries.failure("blarg")
  private def worksAfter[A](howManyTries: Int, returning: A): () => Try[A] = {
    var count = 0
    
    () => {
      if(count >= howManyTries) { Success(returning) }
      else { try { Tries.failure("blarg") } finally { count += 1 } }
    }
  }
  
  private def waitForFirst[A](obs: Observable[A]): A = {
    import Observables.Implicits._
    
    TestHelpers.waitFor(obs.firstAsFuture)
  }
  
  {
    import Loops.retryUntilSuccessWithBackoff
    
    test("retryUntilSuccessWithBackoff - 0 times") {
      assert(retryUntilSuccessWithBackoff(0, 0.1.seconds, 0.5.seconds)(alwaysWorks) === None)
      assert(retryUntilSuccessWithBackoff(0, 0.1.seconds, 0.5.seconds)(alwaysFails) === None)
      
      val willWorkEventually = worksAfter(1, "foo")
      
      assert(retryUntilSuccessWithBackoff(0, 0.01.seconds, 0.05.seconds)(willWorkEventually()) === None)
    }
    
    test("retryUntilSuccessWithBackoff - 1 times") {
      assert(retryUntilSuccessWithBackoff(1, 0.01.seconds, 0.05.seconds)(alwaysWorks) === Some(42))
      assert(retryUntilSuccessWithBackoff(1, 0.01.seconds, 0.05.seconds)(alwaysFails) === None)
      
      val willWorkEventually = worksAfter(1, "foo")
      
      assert(retryUntilSuccessWithBackoff(1, 0.01.seconds, 0.05.seconds)(willWorkEventually()) === None)
    }
    
    test("retryUntilSuccessWithBackoff - 3 times") {
      assert(retryUntilSuccessWithBackoff(3, 0.01.seconds, 0.05.seconds)(alwaysWorks) === Some(42))
      assert(retryUntilSuccessWithBackoff(3, 0.01.seconds, 0.05.seconds)(alwaysFails) === None)
      
      val willWorkEventually = worksAfter(2, "foo")
      
      assert(retryUntilSuccessWithBackoff(3, 0.01.seconds, 0.05.seconds)(willWorkEventually()) === Some("foo"))
    }
  }
  
  {
    import Loops.retryUntilSuccessWithBackoffAsync
    import TestHelpers.waitFor
    import scala.concurrent.ExecutionContext.Implicits.global
    
    test("retryUntilSuccessWithBackoffAsync - 0 times") {
      assert(waitForFirst(retryUntilSuccessWithBackoffAsync(0, 0.1.seconds, 0.5.seconds)(alwaysWorks)) === None)
      assert(waitForFirst(retryUntilSuccessWithBackoffAsync(0, 0.1.seconds, 0.5.seconds)(alwaysFails)) === None)
      
      val willWorkEventually = worksAfter(1, "foo")
      
      assert(
          waitForFirst(retryUntilSuccessWithBackoffAsync(0, 0.01.seconds, 0.05.seconds)(willWorkEventually())) === None)
    }
    
    test("retryUntilSuccessWithBackoffAsync - 1 times") {
      assert(waitForFirst(retryUntilSuccessWithBackoffAsync(1, 0.01.seconds, 0.05.seconds)(alwaysWorks)) === Some(42))
      assert(waitForFirst(retryUntilSuccessWithBackoffAsync(1, 0.01.seconds, 0.05.seconds)(alwaysFails)) === None)
      
      val willWorkEventually = worksAfter(1, "foo")
      
      assert(
          waitForFirst(retryUntilSuccessWithBackoffAsync(1, 0.01.seconds, 0.05.seconds)(willWorkEventually())) === None)
    }
    
    test("retryUntilSuccessWithBackoffAsync - 3 times") {
      assert(waitForFirst(retryUntilSuccessWithBackoffAsync(3, 0.01.seconds, 0.05.seconds)(alwaysWorks)) === Some(42))
      assert(waitForFirst(retryUntilSuccessWithBackoffAsync(3, 0.01.seconds, 0.05.seconds)(alwaysFails)) === None)
      
      val willWorkEventually = worksAfter(2, "foo")
      
      assert(
        waitForFirst(
          retryUntilSuccessWithBackoffAsync(3, 0.01.seconds, 0.05.seconds)(willWorkEventually())) === Some("foo"))
    }
  }
  
  test("delaySequence") {
    import Loops.Backoff.delaySequence
    
    val expected = Seq(
        0.5.seconds,
        1.0.seconds,
        2.0.seconds,
        4.0.seconds,
        8.0.seconds,
        16.0.seconds,
        30.0.seconds,
        30.0.seconds,
        30.0.seconds)
        
    assert(delaySequence(0.5.seconds, 30.seconds).take(9).toIndexedSeq === expected)
  }
}
