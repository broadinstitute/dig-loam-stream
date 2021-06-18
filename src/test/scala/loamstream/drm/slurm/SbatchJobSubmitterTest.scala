package loamstream.drm.slurm

import org.scalatest.FunSuite

/**
 * @author clint
 * May 25, 2021
 */
final class SbatchJobSubmitterTest extends FunSuite {
  test("submitJobs") {
    fail("TODO")
  }
  
  test("extractJobId") {
    import SbatchJobSubmitter.extractJobId
    
    assert(extractJobId(Nil) === None)
    assert(extractJobId(Seq("asdasdda")) === None)
    assert(extractJobId(Seq("asdasdda", "", "aksldfjlas")) === None)
    
    assert(extractJobId(Seq("Submitted batch job 42")) === Some("42"))
  }
}