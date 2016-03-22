package utils

import loamstream.TestHelpers
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Mar 10, 2016
  */
final class LoamFileUtilsTest extends FunSuite {
  test("Relative paths can be resolved properly") {
    val samples = LoamFileUtils.resolveRelativePath("samples.txt").get

    val miniVcf = LoamFileUtils.resolveRelativePath("mini.vcf").get

    import TestHelpers.path

    assert(samples.getFileName === path("samples.txt"))

    assert(samples.toFile.exists)

    assert(miniVcf.getFileName === path("mini.vcf"))

    assert(miniVcf.toFile.exists)
  }
}

