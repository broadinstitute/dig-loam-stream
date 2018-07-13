package loamstream.model.jobs

import java.net.URI
import java.nio.file.Paths
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.googlecloud.CloudStorageClient
import loamstream.model.jobs.Output.GcsUriOutput
import loamstream.model.jobs.OutputTest.MockGcsClient
import loamstream.util.Hash
import loamstream.util.HashType
import loamstream.util.HashType.Md5
import loamstream.util.HashType.Sha1
import loamstream.util.PathUtils
import loamstream.util.PlatformUtil
import java.nio.file.Path
import loamstream.TestHelpers

/**
  * @author clint
  *         kyuksel
  * Aug 5, 2016
  */
final class OutputTest extends FunSuite {
  test("PathOutput") {
    import Output.PathOutput
    import TestHelpers.path
    import java.nio.file.Files
    import PathUtils.normalizePath

    val nonExistingLocation = "sjkafhkfhksdjfh"
    val nonExistingPath = path(nonExistingLocation)
      
    //sanity check
    assert(Files.exists(nonExistingPath) === false)
  
    def doTest(existingPath: Path): Unit = {
      //sanity check
      assert(Files.exists(existingPath))
      
      val doesntExist = PathOutput(path(nonExistingLocation))
      val exists = PathOutput(existingPath)  
  
      assert(exists.pathInHost === normalizePath(existingPath))
      
      assert(doesntExist.pathInHost === normalizePath(nonExistingPath))
      
      assert(!doesntExist.isPresent)
  
      intercept[Exception] {
        doesntExist.hash.get
      }
  
      assert(exists.isPresent, s"'${exists.pathInHost}' doesn't exist")
      
      val expectedHash = {
        //NB: These are different on different platforms due to GitHub's line-ending-munging. 
        if (PlatformUtil.isWindows) { "kUUgk+jLmf99lY+xeUH/MX0CYxg=" } 
        else { "y3i4QSra98i17swJ28mqTTy7NnU=" }
      }
  
      val hashStr = exists.hash.get.valueAsBase64String
      
      assert(hashStr == expectedHash)
  
      val doesntExistRecord = doesntExist.toOutputRecord
      
      val expectedDoesntExistRecord = OutputRecord( 
          loc = PathUtils.normalize(nonExistingPath),
          isPresent = false,
          hash = None,
          hashType = None,
          lastModified = None)
                                                    
      assert(doesntExistRecord === expectedDoesntExistRecord)
  
      val existsRecord = exists.toOutputRecord
      
      val expectedExistsRecord = OutputRecord(
          loc = PathUtils.normalize(existingPath),
          isPresent = true,
          hash = Some(hashStr),
          hashType = Some(Sha1.algorithmName),
          lastModified = Some(PathUtils.lastModifiedTime(existingPath)))
      
      assert(existsRecord === expectedExistsRecord)
      
    }
    
    doTest(path("src/test/resources/for-hashing/foo.txt"))
  }

  test("GcsUriOutput.location") {
    import java.net.URI
    import Output.GcsUriOutput

    val invalidLocation = "sjkafhkfhksdjfh"
    val invalidUri = URI.create(invalidLocation)
    val invalidOutput = GcsUriOutput(invalidUri, Option(MockGcsClient()))

    assert(invalidOutput.location === invalidLocation)

    val validLocation = "gs://bucket/folder/file"
    val validUri = URI.create(validLocation)
    val validOutput = GcsUriOutput(validUri, Option(MockGcsClient()))

    assert(validOutput.location === validLocation)
  }

  val someLoc = "gs://bucket/folder/file"
  val someURI = URI.create(someLoc)

  test("GcsUriOutput with no CloudStorageClient") {
    val output = GcsUriOutput(someURI, client = None)
    val expectedOutputRecord = OutputRecord(loc = someLoc,
                                            isPresent = false,
                                            hash = None,
                                            hashType = None,
                                            lastModified = None)

    assert(output.isMissing)
    assert(output.hash.isEmpty)
    assert(output.lastModified.isEmpty)
    assert(output.toOutputRecord === expectedOutputRecord)
  }

  test("GcsUriOutput with CloudStorageClient") {
    def gcsUriOutput(hash: Option[Hash] = None,
                     isPresent: Boolean = false,
                     lastModified: Option[Instant] = None) = {
      GcsUriOutput(someURI, Option(MockGcsClient(hash, isPresent, lastModified)))
    }

    val someHash = Hash.fromStrings(Some("HashValue"), Md5.algorithmName)

    // Not present; no hash; no timestamp
    val output1 = gcsUriOutput()
    val expectedOutputRecord1 = OutputRecord( loc = someLoc,
                                              isPresent = false,
                                              hash = None,
                                              hashType = None,
                                              lastModified = None)
    assert(output1.isMissing)
    assert(output1.hash.isEmpty)
    assert(output1.lastModified.isEmpty)
    assert(output1.toOutputRecord === expectedOutputRecord1)

    // Present; no hash; no timestamp
    val output2 = gcsUriOutput(isPresent = true)
    val expectedOutputRecord2 = OutputRecord( loc = someLoc,
                                              isPresent = true,
                                              hash = None,
                                              hashType = None,
                                              lastModified = None)
    assert(output2.isPresent)
    assert(output2.hash.isEmpty)
    assert(output2.lastModified.isEmpty)
    assert(output2.toOutputRecord === expectedOutputRecord2)

    // Present; some hash; no timestamp
    val output3 = gcsUriOutput(isPresent = true, hash = someHash)
    val expectedOutputRecord3 = OutputRecord( loc = someLoc,
                                              isPresent = true,
                                              hash = someHash.map(_.valueAsBase64String),
                                              hashType = Some(Md5.algorithmName),
                                              lastModified = None)
    assert(output3.isPresent)
    assert(output3.hash.isDefined)
    assert(output3.lastModified.isEmpty)
    assert(output3.toOutputRecord === expectedOutputRecord3)

    // Present; some hash; some timestamp
    val output4 = gcsUriOutput(isPresent = true, hash = someHash, lastModified = Some(Instant.ofEpochMilli(2)))
    val expectedOutputRecord4 = OutputRecord( loc = someLoc,
                                              isPresent = true,
                                              hash = someHash.map(_.valueAsBase64String),
                                              hashType = Some(Md5.algorithmName),
                                              lastModified = Some(Instant.ofEpochMilli(2)))
    assert(output4.isPresent)
    assert(output4.hash.isDefined)
    assert(output4.lastModified.isDefined)
    assert(output4.toOutputRecord === expectedOutputRecord4)
  }
}

object OutputTest {
  final case class MockGcsClient(hash: Option[Hash] = None,
                                 isPresent: Boolean = false,
                                 lastModified: Option[Instant] = None)
  extends CloudStorageClient {
    override val hashAlgorithm: HashType = Md5

    override def hash(uri: URI): Option[Hash] = hash

    override def isPresent(uri: URI): Boolean = isPresent

    override def lastModified(uri: URI): Option[Instant] = lastModified
  }
}
