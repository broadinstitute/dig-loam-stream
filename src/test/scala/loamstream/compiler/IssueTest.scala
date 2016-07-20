package loamstream.compiler

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Jul 20, 2016
 */
final class IssueTest extends FunSuite {
  test("Severity.apply") {
    intercept[Exception] {
      Issue.Severity(-100)
    }
    
    intercept[Exception] {
      Issue.Severity(-1)
    }
    
    intercept[Exception] {
      Issue.Severity(3)
    }
    
    intercept[Exception] {
      Issue.Severity(42)
    }
    
    assert(Issue.Severity(0) == Issue.Severity.Info)
    assert(Issue.Severity(1) == Issue.Severity.Warning)
    assert(Issue.Severity(2) == Issue.Severity.Error)
  }
}