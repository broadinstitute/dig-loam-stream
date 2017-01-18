package loamstream.model.jobs

import java.nio.file.Paths

import loamstream.util.{PathUtils, PlatformUtil}
import org.scalatest.FunSuite

/**
  * @author clint
  *         kyuksel
  * Aug 5, 2016
  */
final class OutputTest extends FunSuite {
  test("PathOutput") {
    import Output.PathOutput

    val nonExistingLocation = "sjkafhkfhksdjfh"
    val doesntExist = PathOutput(Paths.get(nonExistingLocation))

    val existingPath = Paths.get("src/test/resources/for-hashing/foo.txt")
    val exists = PathOutput(existingPath)

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

    val hashStr = exists.hash.get.valueAsHexString
    assert(hashStr == expectedHash)

    val doesntExistRecord = doesntExist.toOutputRecord
    val expectedDoesntExistRecord = OutputRecord( loc = PathUtils.normalize(Paths.get(nonExistingLocation)),
                                                  isPresent = false,
                                                  hash = None,
                                                  lastModified = None)
    assert(doesntExistRecord === expectedDoesntExistRecord)

    val existsRecord = exists.toOutputRecord
    val expectedExistsRecord = OutputRecord(loc = PathUtils.normalize(existingPath),
                                            isPresent = true,
                                            hash = Some(hashStr),
                                            lastModified = Some(PathUtils.lastModifiedTime(existingPath)))
    assert(existsRecord === expectedExistsRecord)
  }

  test("GcsUriOutput.location") {
    import java.net.URI
    import Output.GcsUriOutput

    val invalidLocation = "sjkafhkfhksdjfh"
    val invalidUri = URI.create(invalidLocation)
    val invalidOutput = GcsUriOutput(invalidUri)

    assert(invalidOutput.location === invalidLocation)

    val validLocation = "gs://bucket/folder/file"
    val validUri = URI.create(validLocation)
    val validOutput = GcsUriOutput(validUri)

    assert(validOutput.location === validLocation)
  }
}
