package loamstream.drm

import org.scalatest.FunSuite

/**
 * @author clint
 * May 23, 2018
 */
final class DrmSystemTest extends FunSuite {
  test("isUger/isLsf") {
    import DrmSystem._
    
    assert(Uger.isUger === true)
    assert(Uger.isLsf === false)
    
    assert(Lsf.isUger === false)
    assert(Lsf.isLsf === true)
  }
}
