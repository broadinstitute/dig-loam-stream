package loamstream.model.jobs

import org.scalatest.FunSuite
import java.nio.file.Paths

/**
 * @author clint
 * date: Aug 5, 2016
 */
final class OutputTest extends FunSuite {
  test("PathOutput") {
    import Output.PathOutput
    
    val doesntExist = PathOutput(Paths.get("sjkafhkfhksdjfh"))
    val exists = PathOutput(Paths.get("src/test/resources/for-hashing/foo.txt"))
    
    assert(!doesntExist.isPresent)
    
    intercept[Exception] {
      doesntExist.hash
    }
    
    assert(exists.isPresent)
    assert(exists.hash.valueAsHexString == "cb78b8412adaf7c8b5eecc09dbc9aa4d3cbb3675")
  }
}