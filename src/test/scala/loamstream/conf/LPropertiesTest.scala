package loamstream.conf

import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Mar 10, 2016
  */
final class LPropertiesTest extends FunSuite {
  test("Config files are correctly parsed") {
    val props = LProperties.load("loamstream-test")

    assert(props.getString("sampleFiles.vcf.mini") == Some("src/test/resources/mini.vcf"))

    //not present
    assertResult(None)(props.getString(null)) // scalastyle:ignore null
    assertResult(None)(props.getString(""))
    assertResult(None)(props.getString("loamstream.foo"))
    assertResult(None)(props.getString("foo"))

    //not strings
    assertResult(None)(props.getString("sampleFiles.vcf"))
    assertResult(None)(props.getString("sampleFiles"))
  }
}