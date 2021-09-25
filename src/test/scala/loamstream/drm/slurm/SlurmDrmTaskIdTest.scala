package loamstream.drm.slurm

import org.scalatest.FunSuite
import loamstream.drm.DrmTaskId

/**
  * @author clint
  * @date Aug 3, 2021
  *
  */
final class SlurmDrmTaskIdTest extends FunSuite {
  test("parseDrmTaskIds - happy paths") {
    import SlurmDrmTaskId.parseDrmTaskIds

    assert(parseDrmTaskIds("123_42").get === Seq(DrmTaskId("123", 42)))

    assert(parseDrmTaskIds("123_[3-3]").get === Seq(DrmTaskId("123", 3)))

    assert(parseDrmTaskIds("123_[3-5]").get === Seq(DrmTaskId("123", 3), DrmTaskId("123", 4), DrmTaskId("123", 5)))
  }

  test("parseDrmTaskIds - bad input") {
    import SlurmDrmTaskId.parseDrmTaskIds

    assert(parseDrmTaskIds("").isFailure)
    assert(parseDrmTaskIds("asdasf").isFailure)
    assert(parseDrmTaskIds("asdf_").isFailure)
    assert(parseDrmTaskIds("foo_asdf").isFailure)
    assert(parseDrmTaskIds("foo_1-3").isFailure)
    assert(parseDrmTaskIds("foo_[]").isFailure)
    assert(parseDrmTaskIds("foo_[1]").isFailure)

    assert(parseDrmTaskIds("").isFailure)
    assert(parseDrmTaskIds("123").isFailure)
    assert(parseDrmTaskIds("123_").isFailure)
    assert(parseDrmTaskIds("123_asdf").isFailure)
    assert(parseDrmTaskIds("123_1-3").isFailure)
    assert(parseDrmTaskIds("123_[]").isFailure)
    assert(parseDrmTaskIds("123_[1]").isFailure)
  }
}
