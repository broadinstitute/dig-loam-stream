package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * date: Mar 10, 2016
 */
final class TypesafeConfigLpropertiesTest extends FunSuite {
  test("Keys should be qualified properly") {
    assert(TypesafeConfigLproperties.qualifiedKey("foo") == "loamstream.foo")
    assert(TypesafeConfigLproperties.qualifiedKey("foo.bar.baz") == "loamstream.foo.bar.baz")
    
    intercept[Exception] {
      TypesafeConfigLproperties.qualifiedKey("  ")
    }
    
    intercept[Exception] {
      TypesafeConfigLproperties.qualifiedKey("")
    }
    
    intercept[Exception] {
      TypesafeConfigLproperties.qualifiedKey(null)  // scalastyle:ignore null
    }
  }
  
  test("getAs()") {
    val props = TypesafeConfigLproperties(ConfigFactory.load("loamstream-test"))
    
    val s: String = props.getString("sampleFiles.samples").get
    
    assert(s == "samples.txt")
    
    val p: Path = props.getPath("sampleFiles.samples").get
    
    assert(p == Paths.get("samples.txt"))
    
    assert(props.getString("sampleFiles").isEmpty)
    assert(props.getString("foo").isEmpty)
    
    assert(props.getPath("sampleFiles").isEmpty)
    assert(props.getPath("foo").isEmpty)
  }
}