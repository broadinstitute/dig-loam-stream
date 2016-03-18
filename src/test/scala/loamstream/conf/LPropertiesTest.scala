package loamstream.conf

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Mar 10, 2016
 */
final class LPropertiesTest extends FunSuite {
  test("Config files are correctly parsed") {
    val props = LProperties.load("loamstream-test")
    
    assert(props.getString("sampleFiles.vcf.mini").contains("mini.vcf"))
    assert(props.getString("sampleFiles.samples").contains("samples.txt"))
    
    //not present
    assert(props.getString(null).isEmpty)  // scalastyle:ignore null
    assert(props.getString("").isEmpty)
    assert(props.getString("loamstream.foo").isEmpty)
    assert(props.getString("foo").isEmpty)
    
    //not strings
    assert(props.getString("sampleFiles.vcf").isEmpty)
    assert(props.getString("sampleFiles").isEmpty)
  }
}