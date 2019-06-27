package loamstream.googlecloud

import org.scalatest.FunSuite
import java.net.URI
import loamstream.IntegrationTestHelpers
import org.scalactic.source.Position.apply

/**
 * @author clint
 * Jun 25, 2019
 */
final class GcsClientAndDriverTest extends FunSuite {
  import IntegrationTestHelpers.path
  
  private val bucketName = "loamstream"
  private val testDataDir = "integration-tests"
  
  //private val credentialFile = path("/humgen/diabetes/users/dig/google_credential.json")
  private val credentialFile = path("/home/clint/workspace/google_credential.json")
  
  test("blobsAt") {
    val driver = new GcsCloudStorageDriver(credentialFile)
    
    val dirName = "foo/bar/"
    
    val k0 = "foo/bar/baz"
    val k1 = "foo/bar/blerg"
    val k2 = "foo/bar/zerg"
    
    deleteTestDataAndThen(driver, "blobsAt") { (subKey, uriOf) =>
      driver.put(uriOf(k0), "asdf")
      driver.put(uriOf(k1), "xyz")
      driver.put(uriOf(k2), "abc")
      
      {
        val blobs = driver.blobsAt(uriOf("foo/"))
        
        assert(blobs.map(_.name).toSet === Set(subKey(k0), subKey(k1), subKey(k2)))
      }
      
      {
        val blobs = driver.blobsAt(uriOf(dirName))
        
        assert(blobs.map(_.name).toSet === Set(subKey(k0), subKey(k1), subKey(k2)))
      }
      
      {
        val blobs = driver.blobsAt(uriOf(k0))
        
        assert(blobs.map(_.name).toSet === Set(subKey(k0)))
      }
      
      {
        val blobs = driver.blobsAt(uriOf("foo/bar/baz/blip/zip/trip"))
        
        assert(blobs.isEmpty)
      }
    }
  }
  
  test("isPresent") {
    val driver = new GcsCloudStorageDriver(credentialFile)
    
    val client = new GcsCloudStorageClient(driver)
    
    val dirName = "foo/bar/"
    
    val k0 = s"${dirName}baz"
    val k1 = s"${dirName}blerg"
    val k2 = s"${dirName}zerg"
    
    val emptyDirName = s"${dirName}empty/" 
    
    deleteTestDataAndThen(driver, "isPresent") { (_, uriOf) =>
      driver.put(uriOf(k0), "asdf")
      driver.put(uriOf(k1), "xyz")
      driver.put(uriOf(k2), "abc")
      
      driver.put(uriOf(emptyDirName), "")
      
      assert(client.isPresent(uriOf(k0)))
      assert(client.isPresent(uriOf(k1)))
      assert(client.isPresent(uriOf(k2)))
      
      assert(client.isPresent(uriOf(dirName)))
      assert(client.isPresent(uriOf(emptyDirName)))
      
      assert(client.isPresent(uriOf("foo/bar/baz/blip/zip/trip")) === false)
    }
  }
  
  test("isPresent - problematic dir") {
    val driver = new GcsCloudStorageDriver(credentialFile)
    
    deleteTestDataAndThen(driver, "isPresentProblematicDir") { (subKey, uriOf) =>
    
      //There is an implicit dir 'foo', as well as the file 'foo.xyz'; both should exist according to isPresent
      driver.put(uriOf("foo.xyz"), "asdf")
      
      driver.put(uriOf("foo/bar"), "asdf")
      driver.put(uriOf("foo/baz"), "asdf")
      driver.put(uriOf("foo/blerg"), "asdf")
      
      val client = new GcsCloudStorageClient(driver)
    
      assert(client.isPresent(uriOf("foo")))
      assert(client.isPresent(uriOf("foo.xyz")))
    }
  }
  
  test("blobsAt - problematic dir") {
    val driver = new GcsCloudStorageDriver(credentialFile)
    
    deleteTestDataAndThen(driver, "isPresentProblematicDir") { (subKey, uriOf) =>
    
      driver.put(uriOf("foo.xyz"), "asdf")
      
      driver.put(uriOf("foo/bar"), "asdf")
      driver.put(uriOf("foo/baz"), "asdf")
      driver.put(uriOf("foo/blerg"), "asdf")
      
      assert(driver.blobsAt(uriOf("foo/")).size === 3)
      assert(driver.blobsAt(uriOf("foo")).size === 3)
      assert(driver.blobsAt(uriOf("foo.xyz")).size === 1)
    }
  }
  
  private def deleteTestDataAndThen[A](
      driver: GcsCloudStorageDriver, 
      testName: String)(f: (String => String, String => URI) => A) {
    val testSubDir = s"${testDataDir}/${testName}"
    
    driver.deleteWithPrefix(bucketName, testSubDir)
    
    def subKey(key: String): String = s"${testSubDir}/${key}"
  
    def uriOf(key: String): URI = URI.create(s"gs://${bucketName}/${subKey(key)}")
    
    f(subKey, uriOf)
  }
}
