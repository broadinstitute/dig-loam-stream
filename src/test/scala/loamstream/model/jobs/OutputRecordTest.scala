package loamstream.model.jobs

import java.nio.file.{Path, Paths}
import java.time.Instant

import loamstream.model.jobs.Output.PathOutput
import loamstream.util.{Hashes, PathUtils}
import org.scalatest.FunSuite

/**
 * @author kyuksel
 * date: Jan 3, 2017
 */
class OutputRecordTest extends FunSuite {
  private val fooLoc = normalize("src/test/resources/for-hashing/foo.txt")
  private val fooPath = Paths.get(fooLoc)
  private val fooHash = Hashes.sha1(fooPath).valueAsHexString
  private val fooRec = OutputRecord(fooLoc, Option(fooHash), lastModifiedOptOf(fooPath))

  private val fooPathCopy = fooPath
  private val fooHashCopy = Hashes.sha1(fooPathCopy).valueAsHexString
  private val fooRecCopy = OutputRecord(fooLoc, Option(fooHashCopy), lastModifiedOptOf(fooPathCopy))

  private val emptyLoc = normalize("src/test/resources/for-hashing/empty.txt")
  private val emptyPath = Paths.get(emptyLoc)
  private val emptyHash = Hashes.sha1(emptyPath).valueAsHexString
  private val emptyRec = OutputRecord(emptyLoc, Option(emptyHash), lastModifiedOptOf(emptyPath))

  private val nonExistingLoc = normalize("non/existent/path")
  private val nonExistingPath = Paths.get(nonExistingLoc)
  private val nonExistingRec = OutputRecord(nonExistingLoc)

  private def lastModifiedOptOf(p: Path): Option[Instant] = Option(PathUtils.lastModifiedTime(p))

  private def normalize(loc: String): String = PathUtils.normalize(Paths.get(loc))

  test("apply/isPresent/isMissing") {
    assert(fooRec.isPresent)

    assert(nonExistingRec.isMissing)

    val recFromFooOutput = OutputRecord(PathOutput(fooPath).normalized)
    assert(fooRec == recFromFooOutput)

    val recFromNonExistingOutput = OutputRecord(PathOutput(nonExistingPath).normalized)
    val expectedNonExistingRec = OutputRecord(nonExistingLoc, false, None, None)
    assert(nonExistingRec == expectedNonExistingRec)
  }

  test("hasDifferentHashThan") {
    assert(!fooRec.hasDifferentHashThan(fooRecCopy))
    assert(fooRec.hasDifferentHashThan(emptyRec))
  }

  test("isOlderThan/withLastModified") {
    assert(!fooRec.isOlderThan(fooRecCopy))
    assert(!fooRec.isOlderThan(nonExistingRec))

    val newRec = fooRec.withLastModified(Instant.now())
    assert(fooRec.isOlderThan(newRec))
  }

}
