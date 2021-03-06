package loamstream.model.jobs

import java.net.URI
import java.nio.file.Path
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.googlecloud.CloudStorageClient
import loamstream.model.jobs.DataHandle.GcsUriHandle
import loamstream.model.jobs.DataHandleTest.MockGcsClient
import loamstream.util.Hash
import loamstream.util.HashType
import loamstream.util.HashType.Md5
import loamstream.util.HashType.Sha1
import loamstream.util.Paths
import loamstream.util.PlatformUtil

/**
  * @author clint
  *         kyuksel
  * Aug 5, 2016
  */
final class DataHandleTest extends FunSuite {
  test("PathHandle") {
    import DataHandle.PathHandle
    import TestHelpers.path
    import java.nio.file.Files
    import Paths.normalizePath

    val nonExistingLocation = "sjkafhkfhksdjfh"
    val nonExistingPath = path(nonExistingLocation)
      
    val doesntExist = PathHandle(path(nonExistingLocation))
    
    //sanity check
    assert(Files.exists(nonExistingPath) === false)
  
    def doTest(existingPath: Path): Unit = {
      //sanity check
      assert(Files.exists(existingPath))
      
      val exists = PathHandle(existingPath)  
  
      assert(exists.path === normalizePath(existingPath))
      
      assert(doesntExist.path === normalizePath(nonExistingPath))
      
      assert(!doesntExist.isPresent)
  
      assert(doesntExist.hash.isEmpty)
  
      assert(exists.isPresent, s"'${exists.path}' doesn't exist")
      
      //NB: These are different on different platforms due to GitHub's line-ending-munging.
      val expectedHash = if (PlatformUtil.isWindows) "kUUgk+jLmf99lY+xeUH/MX0CYxg=" else "y3i4QSra98i17swJ28mqTTy7NnU="
  
      val hashStr = exists.hash.get.valueAsBase64String
      
      assert(hashStr == expectedHash)
  
      val doesntExistRecord = doesntExist.toStoreRecord
      
      val expectedDoesntExistRecord = StoreRecord( 
          loc = Paths.normalize(nonExistingPath),
          isPresent = false,
          makeHash = () => None,
          makeHashType = () => None,
          lastModified = None)
                                                    
      assert(doesntExistRecord === expectedDoesntExistRecord)
  
      val existsRecord = exists.toStoreRecord
      
      val expectedExistsRecord = StoreRecord(
          loc = Paths.normalize(existingPath),
          isPresent = true,
          makeHash = () => Some(hashStr),
          makeHashType = () => Some(Sha1.algorithmName),
          lastModified = Some(Paths.lastModifiedTime(existingPath)))
      
      assert(existsRecord === expectedExistsRecord)
    }
    
    doTest(path("src/test/resources/for-hashing/foo.txt"))
  }

  test("GcsUriHandle.location") {
    import java.net.URI
    import DataHandle.GcsUriHandle

    val invalidLocation = "sjkafhkfhksdjfh"
    val invalidUri = URI.create(invalidLocation)
    val invalidOutput = GcsUriHandle(invalidUri, Option(MockGcsClient()))

    assert(invalidOutput.location === invalidLocation)

    val validLocation = "gs://bucket/folder/file"
    val validUri = URI.create(validLocation)
    val validOutput = GcsUriHandle(validUri, Option(MockGcsClient()))

    assert(validOutput.location === validLocation)
  }

  val someLoc = "gs://bucket/folder/file"
  val someURI = URI.create(someLoc)

  test("GcsUriHandle with no CloudStorageClient") {
    val output = GcsUriHandle(someURI, client = None)
    val expectedOutputRecord = StoreRecord(loc = someLoc,
                                           isPresent = false,
                                           makeHash = () => None,
                                           makeHashType = () => None,
                                           lastModified = None)

    assert(output.isMissing)
    assert(output.hash.isEmpty)
    assert(output.lastModified.isEmpty)
    assert(output.toStoreRecord === expectedOutputRecord)
  }

  test("GcsUriHandle with CloudStorageClient") {
    def gcsUriOutput(hash: Option[Hash] = None,
                     isPresent: Boolean = false,
                     lastModified: Option[Instant] = None) = {
      GcsUriHandle(someURI, Option(MockGcsClient(hash, isPresent, lastModified)))
    }

    val someHash = Hash.fromStrings(Some("HashValue"), Md5.algorithmName)

    // Not present; no hash; no timestamp
    val output1 = gcsUriOutput()
    val expectedOutputRecord1 = StoreRecord(loc = someLoc,
                                            isPresent = false,
                                            makeHash = () => None,
                                            makeHashType = () => None,
                                            lastModified = None)
    assert(output1.isMissing)
    assert(output1.hash.isEmpty)
    assert(output1.lastModified.isEmpty)
    assert(output1.toStoreRecord === expectedOutputRecord1)

    // Present; no hash; no timestamp
    val output2 = gcsUriOutput(isPresent = true)
    val expectedOutputRecord2 = StoreRecord(loc = someLoc,
                                            isPresent = true,
                                            makeHash = () => None,
                                            makeHashType = () => None,
                                            lastModified = None)
    assert(output2.isPresent)
    assert(output2.hash.isEmpty)
    assert(output2.lastModified.isEmpty)
    assert(output2.toStoreRecord === expectedOutputRecord2)

    // Present; some hash; no timestamp
    val output3 = gcsUriOutput(isPresent = true, hash = someHash)
    val expectedOutputRecord3 = StoreRecord( loc = someLoc,
                                              isPresent = true,
                                              makeHash = () => someHash.map(_.valueAsBase64String),
                                              makeHashType = () => Some(Md5.algorithmName),
                                              lastModified = None)
    assert(output3.isPresent)
    assert(output3.hash.isDefined)
    assert(output3.lastModified.isEmpty)
    assert(output3.toStoreRecord === expectedOutputRecord3)

    // Present; some hash; some timestamp
    val output4 = gcsUriOutput(isPresent = true, hash = someHash, lastModified = Some(Instant.ofEpochMilli(2)))
    val expectedOutputRecord4 = StoreRecord(loc = someLoc,
                                            isPresent = true,
                                            makeHash = () => someHash.map(_.valueAsBase64String),
                                            makeHashType = () => Some(Md5.algorithmName),
                                            lastModified = Some(Instant.ofEpochMilli(2)))
    assert(output4.isPresent)
    assert(output4.hash.isDefined)
    assert(output4.lastModified.isDefined)
    assert(output4.toStoreRecord === expectedOutputRecord4)
  }
}

object DataHandleTest {
  final case class MockGcsClient(hash: Option[Hash] = None,
                                 isPresent: Boolean = false,
                                 lastModified: Option[Instant] = None) extends CloudStorageClient {
    
    override val hashAlgorithm: HashType = Md5

    override def hash(uri: URI): Option[Hash] = hash

    override def isPresent(uri: URI): Boolean = isPresent

    override def lastModified(uri: URI): Option[Instant] = lastModified
  }
}
