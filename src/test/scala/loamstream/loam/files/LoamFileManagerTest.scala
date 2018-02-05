package loamstream.loam.files

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.compiler.LoamPredef
import java.nio.file.Path
import loamstream.util.Files
import java.nio.file.Paths
import loamstream.util.BashScript
import java.net.URI

/**
 * @author clint
 * Feb 5, 2018
 */
final class LoamFileManagerTest extends FunSuite {
  private val actualTempFile = Files.tempFile("foo")
  
  private def isTempFile(p: Path): Boolean = p.getParent === actualTempFile.getParent
  
  private def isTempFile(s: String): Boolean = isTempFile(Paths.get(s))
  
  test("getPath - anonymous store") {
    TestHelpers.makeGraph { implicit scriptContext =>
      import LoamPredef._
      import scriptContext.projectContext.fileManager
      
      val anonStore = store
      
      val pathForAnonStore = fileManager.getPath(anonStore)
      
      assert(isTempFile(pathForAnonStore))
      
      assert(pathForAnonStore === fileManager.getPath(anonStore))
    }
  }
  
  test("getPath - store with path") {
    TestHelpers.makeGraph { implicit scriptContext =>
      import LoamPredef._
      import scriptContext.projectContext.fileManager
      
      val p = Paths.get("/foo/bar/baz")
      
      val storeWithPath = store.at(p)
      
      val pathForStore = fileManager.getPath(storeWithPath)
      
      assert(isTempFile(pathForStore) === false)
      
      assert(pathForStore === p)
      
      assert(pathForStore === fileManager.getPath(storeWithPath))
    }
  }
  
  test("getStoreString - anonymous store") {
    TestHelpers.makeGraph { implicit scriptContext =>
      import LoamPredef._
      import scriptContext.projectContext.fileManager
      
      val anonStore = store
      
      val pathForAnonStore = fileManager.getStoreString(anonStore)
      
      assert(isTempFile(pathForAnonStore))
      
      assert(pathForAnonStore === fileManager.getStoreString(anonStore))
    }
  }
  
  test("getStoreString - store with path") {
    TestHelpers.makeGraph { implicit scriptContext =>
      import LoamPredef._
      import scriptContext.projectContext.fileManager
      import BashScript.Implicits._
      
      val p = Paths.get("/foo/bar/baz")
      
      val storeWithPath = store.at(p)
      
      val pathForStore = fileManager.getStoreString(storeWithPath)
      
      assert(isTempFile(pathForStore) === false)
      
      assert(pathForStore === p.render)
      
      assert(pathForStore === fileManager.getStoreString(storeWithPath))
    }
  }
  
  test("getStoreString - store with URI") {
    TestHelpers.makeGraph { implicit scriptContext =>
      import LoamPredef._
      import scriptContext.projectContext.fileManager
      
      val uri = URI.create("gs://foo/bar/baz")
      
      val storeWithUri = store.at(uri)
      
      val uriForStore = fileManager.getStoreString(storeWithUri)
      
      assert(uriForStore === uri.toString)
      
      assert(uriForStore === fileManager.getStoreString(storeWithUri))
    }
  }
}
