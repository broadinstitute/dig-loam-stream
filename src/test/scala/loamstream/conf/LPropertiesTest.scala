package loamstream.conf

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Mar 10, 2016
 */
final class LPropertiesTest extends FunSuite {
  test("Config files are correctly parsed") {
    val props = LProperties.load("loamstream-test")
    
    assert(props.getAs[String]("sampleFiles.vcf.mini") == Some("mini.vcf"))
    assert(props.getAs[String]("sampleFiles.samples") == Some("samples.txt"))
    
    //not present
    assert(props.getAs[String](null) == None)
    assert(props.getAs[String]("") == None)
    assert(props.getAs[String]("loamstream.foo") == None)
    assert(props.getAs[String]("foo") == None)
    
    //not strings
    assert(props.getAs[String]("sampleFiles.vcf") == None)
    assert(props.getAs[String]("sampleFiles") == None) 
  }
}