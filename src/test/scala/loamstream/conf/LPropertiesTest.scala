package loamstream.conf

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Mar 10, 2016
 */
final class LPropertiesTest extends FunSuite {
  test("Config files are correctly parsed") {
    val props = LProperties.load("loamstream-test")
    
    assert(props.getString("sampleFiles.vcf.mini") == Some("mini.vcf"))
    assert(props.getString("sampleFiles.samples") == Some("samples.txt"))
    
    //not present
    assert(props.getString(null) == None)
    assert(props.getString("") == None)
    assert(props.getString("loamstream.foo") == None)
    assert(props.getString("foo") == None)
    
    //not strings
    assert(props.getString("sampleFiles.vcf") == None)
    assert(props.getString("sampleFiles") == None) 
  }
}