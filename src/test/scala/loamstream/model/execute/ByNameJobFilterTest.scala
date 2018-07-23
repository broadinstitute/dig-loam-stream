package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob

/**
 * @author clint
 * Jul 2, 2018
 */
final class ByNameJobFilterTest extends FunSuite {
  private def mockJob(name: String): LJob = MockJob(toReturn = JobStatus.Succeeded, name = name)
  
  private val foo123 = mockJob("foo123")
  private val foo = mockJob("foo")
  private val oneTwoThree = mockJob("123")
  
  private val letters = "[a-z]+".r
  private val numbers = "\\d+".r
  private val twentyThree = "23".r
  private val fs = "f+".r
  private val os = "o+".r
  private val xes = "x+".r
  private val ys = "y+".r
  
  test("allOf") {
    import ByNameJobFilter.allOf
    
    //one 
    val letterFilter = allOf(letters)
    
    assert(letterFilter.shouldRun(foo123))
    assert(letterFilter.shouldRun(foo))
    assert(letterFilter.shouldRun(oneTwoThree) === false)
    
    val numberFilter = allOf(numbers)
    
    assert(numberFilter.shouldRun(foo123))
    assert(numberFilter.shouldRun(foo) === false)
    assert(numberFilter.shouldRun(oneTwoThree))
    
    //many
    val letterAndNumberFilter = allOf(letters, numbers)
    
    assert(letterAndNumberFilter.shouldRun(foo123))
    assert(letterAndNumberFilter.shouldRun(foo) === false)
    assert(letterAndNumberFilter.shouldRun(oneTwoThree) === false)
    
    val fooFilter = allOf(fs, os)
    
    assert(fooFilter.shouldRun(foo123))
    assert(fooFilter.shouldRun(foo))
    assert(fooFilter.shouldRun(oneTwoThree) === false)
    
    //shouldn't match
    
    val justXesFilter = allOf(xes)
    
    assert(justXesFilter.shouldRun(foo123) === false)
    assert(justXesFilter.shouldRun(foo) === false)
    assert(justXesFilter.shouldRun(oneTwoThree) === false)
    
    val foxFilter = allOf(fs, os, xes)
    
    assert(foxFilter.shouldRun(foo123) === false)
    assert(foxFilter.shouldRun(foo) === false)
    assert(foxFilter.shouldRun(oneTwoThree) === false)
  }
  
  test("anyOf") {
    import ByNameJobFilter.anyOf
    
    //one 
    val letterFilter = anyOf(letters)
    
    assert(letterFilter.shouldRun(foo123))
    assert(letterFilter.shouldRun(foo))
    assert(letterFilter.shouldRun(oneTwoThree) === false)
    
    val numberFilter = anyOf(numbers)
    
    assert(numberFilter.shouldRun(foo123))
    assert(numberFilter.shouldRun(foo) === false)
    assert(numberFilter.shouldRun(oneTwoThree))
    
    //many
    val letterAndNumberFilter = anyOf(letters, numbers)
    
    assert(letterAndNumberFilter.shouldRun(foo123))
    assert(letterAndNumberFilter.shouldRun(foo))
    assert(letterAndNumberFilter.shouldRun(oneTwoThree))
    
    val foxFilter = anyOf(fs, os, xes)
    
    assert(foxFilter.shouldRun(foo123))
    assert(foxFilter.shouldRun(foo))
    assert(foxFilter.shouldRun(oneTwoThree) === false)
    
    //shouldn't match
    
    val xsAndYsFilter = anyOf(xes, ys)
    
    assert(xsAndYsFilter.shouldRun(foo123) === false)
    assert(xsAndYsFilter.shouldRun(foo) === false)
    assert(xsAndYsFilter.shouldRun(oneTwoThree) === false)
  }
  
  test("noneOf") {
    import ByNameJobFilter.noneOf
    
    //one 
    val letterFilter = noneOf(letters)
    
    assert(letterFilter.shouldRun(foo123) === false)
    assert(letterFilter.shouldRun(foo) === false)
    assert(letterFilter.shouldRun(oneTwoThree))
    
    val numberFilter = noneOf(numbers)
    
    assert(numberFilter.shouldRun(foo123) === false)
    assert(numberFilter.shouldRun(foo))
    assert(numberFilter.shouldRun(oneTwoThree) === false)
    
    //many
    val letterAndNumberFilter = noneOf(letters, numbers)
    
    assert(letterAndNumberFilter.shouldRun(foo123) === false)
    assert(letterAndNumberFilter.shouldRun(foo) === false)
    assert(letterAndNumberFilter.shouldRun(oneTwoThree) === false)
    
    val foxFilter = noneOf(fs, os, xes)
    
    assert(foxFilter.shouldRun(foo123) === false)
    assert(foxFilter.shouldRun(foo) === false)
    assert(foxFilter.shouldRun(oneTwoThree))
    
    val xsAndYsFilter = noneOf(xes, ys)
    
    assert(xsAndYsFilter.shouldRun(foo123))
    assert(xsAndYsFilter.shouldRun(foo))
    assert(xsAndYsFilter.shouldRun(oneTwoThree))
  }
}
