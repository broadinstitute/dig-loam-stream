package utils

import loamstream.TestHelpers
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Mar 10, 2016
  */
final class FileUtilsTest extends FunSuite {
  test("Relative paths can be resolved properly") {
    val miniVcf = FileUtils.resolveRelativePath("mini.vcf").get

    import TestHelpers.path

    assert(miniVcf.getFileName === path("mini.vcf"))

    assert(miniVcf.toFile.exists)
  }
}

