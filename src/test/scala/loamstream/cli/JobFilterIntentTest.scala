package loamstream.cli

import org.scalatest.FunSuite
import loamstream.model.execute.ByNameJobFilter
import loamstream.model.execute.JobFilter
import loamstream.model.execute.MissingOutputsJobFilter

/**
 * @author clint
 * Jul 9, 2019
 */
final class JobFilterIntentTest extends FunSuite {
  import JobFilterIntent._
  
  private val regexes = Seq("a".r, "b".r)
  
  test("AsByNameJobFilter") {
    assert(AsByNameJobFilter.unapply(DontFilterByName) === None)
    assert(AsByNameJobFilter.unapply(RunEverything) === None)
    assert(AsByNameJobFilter.unapply(RunIfAnyMissingOutputs) === None)
    
    assert(AsByNameJobFilter.unapply(RunIfAllMatch(regexes)) === Some(ByNameJobFilter.allOf(regexes)))
    
    assert(AsByNameJobFilter.unapply(RunIfAnyMatch(regexes)) === Some(ByNameJobFilter.anyOf(regexes)))
    
    assert(AsByNameJobFilter.unapply(RunIfNoneMatch(regexes)) === Some(ByNameJobFilter.noneOf(regexes)))
  }
  
  test("toJobFilter") {
    assert(RunEverything.toJobFilter === JobFilter.RunEverything)
    assert(RunIfAnyMissingOutputs.toJobFilter === MissingOutputsJobFilter)
    assert(RunIfAllMatch(regexes).toJobFilter === ByNameJobFilter.allOf(regexes))
    assert(RunIfAnyMatch(regexes).toJobFilter === ByNameJobFilter.anyOf(regexes))
    assert(RunIfNoneMatch(regexes).toJobFilter === ByNameJobFilter.noneOf(regexes))
  }
  
  test("toByNameJobFilter") {
    assert(RunIfAllMatch(regexes).toByNameJobFilter === ByNameJobFilter.allOf(regexes))
    assert(RunIfAnyMatch(regexes).toByNameJobFilter === ByNameJobFilter.anyOf(regexes))
    assert(RunIfNoneMatch(regexes).toByNameJobFilter === ByNameJobFilter.noneOf(regexes))
  }
}
