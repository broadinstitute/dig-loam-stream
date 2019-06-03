package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
 * @author clint
 * Jun 3, 2019
 */
final class TerminationReasonTest extends FunSuite {
  import TerminationReason._
  
  test("name") {
    assert(RunTime.name === "RunTime")
    assert(CpuTime.name === "CpuTime")
    assert(Memory.name === "Memory")
    assert(UserRequested.name === "UserRequested")
    assert(Unknown.name === "Unknown")
  }
  
  private def doFlagTest(
      flag: TerminationReason => Boolean, 
      shouldBeTrueFor: TerminationReason, 
      shouldBeFalseFor: TerminationReason*): Unit = {
    
    assert(flag(shouldBeTrueFor) === true)
    
    shouldBeFalseFor.foreach(tr => assert(flag(tr) === false, s"Expected false for $tr"))
  }
  
  test("isRunTime") {
    doFlagTest(
        _.isRunTime, 
        RunTime, 
        CpuTime, Memory, UserRequested, Unknown)
  }
  
  test("isCpuTime") {
    doFlagTest(
        _.isCpuTime, 
        CpuTime, 
        RunTime, Memory, UserRequested, Unknown)
  }
  
  test("isMemory") {
    doFlagTest(
        _.isMemory, 
        Memory, 
        RunTime, CpuTime, UserRequested, Unknown)
  }
  
  test("isUserRequested") {
    doFlagTest(
        _.isUserRequested, 
        UserRequested, 
        RunTime, CpuTime, Memory, Unknown)
  }
  
  test("isUnknown") {
    doFlagTest(
        _.isUnknown, 
        Unknown, 
        RunTime, CpuTime, Memory, UserRequested)
  }
  
  test("fromName") {
    def doTest(str: String, expected: Option[TerminationReason]): Unit = {
      def actuallyDoTest(s: String): Unit = assert(fromName(s) === expected)

      actuallyDoTest(str)
      actuallyDoTest(s"  $str ")
      actuallyDoTest(str.toUpperCase)
      actuallyDoTest(str.toLowerCase)
      actuallyDoTest(TestHelpers.to1337Speak(str))
    }
    
    doTest("RunTime", Some(RunTime))
    doTest("CpuTime", Some(CpuTime))
    doTest("Memory", Some(Memory))
    doTest("UserRequested", Some(UserRequested))
    doTest("Unknown", Some(Unknown))
    
    doTest("", None)
    doTest("ashdgjasdg", None)
  }
}
