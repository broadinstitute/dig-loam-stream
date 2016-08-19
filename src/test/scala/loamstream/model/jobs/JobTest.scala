package loamstream.model.jobs

import org.scalatest.FunSuite

/**
 * @author clint
 * date: May 27, 2016
 */
final class JobTest extends FunSuite with TestJobs {
  test("chunks()") {
    def jobToString(j: LJob): String = {
      val MockJob(LJob.SimpleSuccess(msg), _, _, _, _) = j
      
      msg
    }
    
    val expected = Seq(
      Set("2(0)", "2(1)"),
      Set("2 + 2"),
      Set("(2 + 2) + 1"))
      
    assert(plusOne.chunks.map(_.map(jobToString)).toIndexedSeq == expected)
  }
  
  test("Result.attempt() (something thrown)") {
    val ex = new Exception("foo")
    
    val failure = LJob.Result.attempt {
      throw ex 
    }
    
    assert(failure == LJob.FailureFromThrowable(ex))
    assert(failure.message == s"Failure! ${ex.getMessage}")
  }
  
  test("Result.attempt() (nothing thrown)") {
    val success = LJob.SimpleSuccess("yay")
    val failure = LJob.SimpleFailure("foo")
    
    assert(LJob.Result.attempt(success) == success)
    
    assert(LJob.Result.attempt(failure) == failure)
  }
  
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
  
  test("remove() on leaves does nothing") {
    assert(two0.remove(two0) eq two0)
    assert(two0.remove(two1) eq two0)
    
    assert(two1.remove(two1) eq two1)
    assert(two1.remove(two0) eq two1)
    
    assert(two0.remove(twoPlusTwo) eq two0)
    assert(two1.remove(twoPlusTwo) eq two1)
    
    assert(two0.remove(plusOne) eq two0)
    assert(two1.remove(plusOne) eq two1)
  }
  
  test("remove()ing self does nothing") {
    assert(two0.remove(two0) eq two0)
    assert(two1.remove(two1) eq two1)
    assert(twoPlusTwo.remove(twoPlusTwo) eq twoPlusTwo)
    assert(plusOne.remove(plusOne) eq plusOne)
  }
  
  /*
   * 2
   *  \
   *   2+2 - (2+2)+1
   *  /
   * 2
   */
  test("remove()") {
    {
      val expected = twoPlusTwo.copy(inputs = Set(two0))
      
      assert(twoPlusTwo.remove(two1) == expected)
    }
    
    {
      val expected = twoPlusTwo.copy(inputs = Set(two1))
      
      assert(twoPlusTwo.remove(two0) == expected)
    }
    
    {
      val expected = plusOne.copy(inputs = Set.empty)
      
      assert(plusOne.remove(twoPlusTwo) == expected)
    }
    
    {
      val expected = plusOne.copy(inputs = Set(twoPlusTwo.copy(inputs = Set(two0))))
      
      assert(plusOne.remove(two1) == expected)
    }
    
    {
      val expected = plusOne.copy(inputs = Set(twoPlusTwo.copy(inputs = Set(two1))))
      
      assert(plusOne.remove(two0) == expected)
    }
  }
  
  /*
   * 2
   *  \
   *   2+2 - (2+2)+1
   *  /
   * 2
   */
  test("removeAll()") {
    assert(two0.removeAll(Set(two0, two1)) eq two0)
    
    assert(two1.removeAll(Set(two0, two1)) eq two1)
    
    assert(two1.removeAll(Set(two0, two1, plusOne)) eq two1)
    assert(two0.removeAll(Set(two0, two1, plusOne)) eq two0)
    
    {
      val expected = plusOne.copy(inputs = Set(twoPlusTwo.copy(inputs = Set())))
      
      assert(plusOne.removeAll(Set(two0, two1)) == expected)
    }
    
    {
      val expected = plusOne.copy(inputs = Set(twoPlusTwo.copy(inputs = Set(two0))))
      
      assert(plusOne.removeAll(Set(two1)) == expected)
    }
  }

}