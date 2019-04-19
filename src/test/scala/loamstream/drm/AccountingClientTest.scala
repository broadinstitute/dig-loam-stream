package loamstream.drm

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 18, 2019
 */
final class AccountingClientTest extends FunSuite {
  import scala.concurrent.duration._
  
  test("delaySequence - defaults") {
    val delays = AccountingClient.delaySequence(
        AccountingClient.defaultDelayStart, 
        AccountingClient.defaultDelayCap)
    
    val actual = delays.take(10).toIndexedSeq
    
    val expected = Seq(
        0.5.seconds, 
        1.second, 
        2.seconds, 
        4.seconds, 
        8.seconds, 
        16.seconds,
        30.seconds,
        30.seconds,
        30.seconds,
        30.seconds)
        
    assert(actual === expected)
  }
  
  test("delaySequence - non-defaults, cap not hit") {
    val delays = AccountingClient.delaySequence(0.01.seconds, 10.seconds)
    
    val actual = delays.take(10).toIndexedSeq
    
    val expected = Seq(
        0.01.seconds, 
        0.02.second, 
        0.04.seconds, 
        0.08.seconds, 
        0.16.seconds, 
        0.32.seconds,
        0.64.seconds,
        1.28.seconds,
        2.56.seconds,
        5.12.seconds)
        
    assert(actual === expected)
  }
  
  test("delaySequence - non-defaults, cap hit") {
    val delays = AccountingClient.delaySequence(0.01.seconds, 1.seconds)
    
    val actual = delays.take(10).toIndexedSeq
    
    val expected = Seq(
        0.01.seconds, 
        0.02.second, 
        0.04.seconds, 
        0.08.seconds, 
        0.16.seconds, 
        0.32.seconds,
        0.64.seconds,
        1.second,
        1.second,
        1.second)
        
    assert(actual === expected)
  }
}
