package loamstream.model.jobs

import java.nio.file.Path
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.model.jobs.DataHandle.PathHandle
import loamstream.util.HashType.Sha1
import loamstream.util.Hashes
import loamstream.util.Paths
import loamstream.TestHelpers

/**
 * @author kyuksel
 * date: Jan 3, 2017
 */
final class StoreRecordTest extends FunSuite {
  import TestHelpers.path
  
  private val fooLoc = normalize("src/test/resources/for-hashing/foo.txt")
  private val fooPath = path(fooLoc)
  private val fooHash = Hashes.sha1(fooPath).valueAsBase64String
  private val fooHashType = Sha1.algorithmName
  private val fooRec = {
    StoreRecord(fooLoc, () => Option(fooHash), () => Option(fooHashType), lastModifiedOptOf(fooPath))
  }

  private val fooPathCopy = fooPath
  private val fooHashCopy = Hashes.sha1(fooPathCopy).valueAsBase64String
  private val fooRecCopy = StoreRecord(fooLoc)

  private val emptyLoc = normalize("src/test/resources/for-hashing/empty.txt")
  private val emptyPath = path(emptyLoc)
  private val emptyHash = Hashes.sha1(emptyPath).valueAsBase64String
  private val emptyHashType = Sha1.algorithmName
  private val emptyRec = {
    StoreRecord(emptyLoc, () => Option(emptyHash), () => Option(emptyHashType), lastModifiedOptOf(emptyPath))
  }

  private val nonExistingLoc = normalize("non/existent/path")
  private val nonExistingRec = StoreRecord(nonExistingLoc)

  private def lastModifiedOptOf(p: Path): Option[Instant] = Option(Paths.lastModifiedTime(p))

  private def normalize(loc: String): String = Paths.normalize(path(loc))

  test("hash and hashType are lazy") {
    var timesHashMade = 0
    var timesHashTypeMade = 0
    
    val bogusHash = Some("bogus-hash")
    val bogusHashType = Some("bogus-hash-type")
    
    val record = StoreRecord(
        loc = "foo",
        isPresent = true,
        makeHash = () => { timesHashMade += 1 ; bogusHash },
        makeHashType = () => { timesHashTypeMade += 1 ; bogusHashType },
        lastModified = Some(Instant.now))
        
    assert(timesHashMade === 0)
    assert(timesHashTypeMade === 0)
    
    assert(record.hash === bogusHash)
    assert(record.hashType === bogusHashType)
    
    assert(timesHashMade === 1)
    assert(timesHashTypeMade === 1)
    
    assert(record.hash === bogusHash)
    assert(record.hashType === bogusHashType)
    
    assert(timesHashMade === 1)
    assert(timesHashTypeMade === 1)
  }
  
  test("apply/isPresent/isMissing") {
    assert(fooRec.isPresent)

    assert(nonExistingRec.isMissing)

    val recFromFooOutput = StoreRecord(PathHandle(fooPath).normalized)
    assert(fooRec == recFromFooOutput)

    val expectedNonExistingRec = StoreRecord(nonExistingLoc, false, () => None, () => None, None)
    assert(nonExistingRec == expectedNonExistingRec)
  }

  test("hasDifferentHashThan") {
    assert(!fooRec.hasDifferentHashThan(fooRecCopy))
    assert(!fooRecCopy.hasDifferentHashThan(fooRec))
    assert(!fooRec.hasDifferentHashThan(fooRec))

    assert(fooRec.hasDifferentHashThan(emptyRec))
    assert(emptyRec.hasDifferentHashThan(fooRec))
  }

  test("hasDifferentModTimeThan/withLastModified") {
    assert(!fooRec.hasDifferentModTimeThan(fooRecCopy))
    assert(!fooRecCopy.hasDifferentModTimeThan(fooRec))
    assert(!fooRec.hasDifferentModTimeThan(nonExistingRec))
    assert(!nonExistingRec.hasDifferentModTimeThan(fooRec))

    val newRec = fooRec.withLastModified(Instant.now())
    
    assert(fooRec.hasDifferentModTimeThan(newRec))
    assert(newRec.hasDifferentModTimeThan(fooRec))
  }

}
