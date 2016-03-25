package loamstream.conf

import loamstream.{TestData, TestHelpers}
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Mar 10, 2016
  */
final class SampleFilesTest extends FunSuite {
  test("Sample files are correctly located") {
    import TestData.sampleFiles
    import TestHelpers.path

    val miniVcfFile = sampleFiles.miniVcfOpt.get

    assert(miniVcfFile.getFileName == path("mini.vcf"))

  }
}