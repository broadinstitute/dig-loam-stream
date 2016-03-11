package utils

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.TestHelpers

/**
 * @author clint
 * date: Mar 10, 2016
 */
final class FileUtilsTest extends FunSuite {
  test("Relative paths can be resolved properly") {
    val samples = FileUtils.resolveRelativePath("samples.txt").get
    
    val miniVcf = FileUtils.resolveRelativePath("mini.vcf").get
    
    import TestHelpers.path
    
    assert(samples.getFileName === path("samples.txt"))
    
    assert(samples.toFile.exists)
    
    assert(miniVcf.getFileName === path("mini.vcf"))
    
    assert(miniVcf.toFile.exists)
  }
}

