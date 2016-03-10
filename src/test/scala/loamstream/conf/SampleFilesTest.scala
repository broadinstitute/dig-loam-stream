package loamstream.conf

import org.scalatest.FunSuite
import loamstream.TestData
import java.nio.file.Paths
import java.nio.file.Files
import org.scalatest.Matchers

/**
 * @author clint
 * date: Mar 10, 2016
 */
final class SampleFilesTest extends FunSuite with Matchers {
  test("Sample files are correctly located") {
    def path(s: String) = Paths.get(s)
    
    import TestData.sampleFiles
    
    val miniVcfFile = sampleFiles.miniVcfOpt.get
    
    assert(miniVcfFile.getFileName === path("mini.vcf"))
    
    val samples = sampleFiles.samplesOpt.get    
    
    assert(samples.getFileName === path("samples.txt"))
  }
}