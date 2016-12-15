package loamstream.model.jobs

import java.nio.file.Paths

import loamstream.util.PlatformUtil
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Aug 5, 2016
  */
final class OutputTest extends FunSuite {
  test("PathOutput") {
    import Output.PathOutput

    val doesntExist = PathOutput(Paths.get("sjkafhkfhksdjfh"))
    val exists = PathOutput(Paths.get("src/test/resources/for-hashing/foo.txt"))

    assert(!doesntExist.isPresent)

    intercept[Exception] {
      doesntExist.hash.get
    }

    assert(exists.isPresent)
    val expectedHash = if (PlatformUtil.isWindows) {
      "91452093e8cb99ff7d958fb17941ff317d026318"
    } else {
      "cb78b8412adaf7c8b5eecc09dbc9aa4d3cbb3675"
    }
    assert(exists.hash.get.valueAsHexString == expectedHash)
  }
}
