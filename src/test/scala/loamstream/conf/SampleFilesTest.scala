package loamstream.conf

import org.scalatest.FunSuite

import loamstream.TestData
import loamstream.TestHelpers

/**
 * @author clint
 * date: Mar 10, 2016
 */
final class SampleFilesTest extends FunSuite {
  test("Sample files are correctly located") {
    import TestHelpers.path
    
    import TestData.sampleFiles
    
    val miniVcfFile = sampleFiles.miniVcfOpt.get
    
    assert(miniVcfFile.getFileName == path("mini.vcf"))
    
    val samples = sampleFiles.samplesOpt.get    
    
    assert(samples.getFileName == path("samples.txt"))
  }
}