package loamstream.loam

import loamstream.loam.files.LoamFileManager
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.model.Store

/** Tests of LoamStoreRef, basically whether the path comes out right */
final class LoamStoreRefTest extends FunSuite {
  private implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty(TestHelpers.config))
  
  private val fileManager = new LoamFileManager
  
  test("Adding a suffix to the path of a store.") {
    val path = "a/b/c/myfile.txt"
    val suffix = ".gz"
    val store = Store.create.at(path).asInput
    val storeRef = store + suffix
    assert(storeRef.path(fileManager).toString === store.pathOpt.get.toString + suffix)
  }
  
  test("Removing a suffix from the path of a store.") {
    val path = "a/b/c/myfile.txt"
    val suffix = ".txt"
    val store = Store.create.at(path).asInput
    val storeRef = store - suffix
    assert(storeRef.path(fileManager).toString + suffix === store.pathOpt.get.toString)
  }
  
  test("Try removing a non-existing suffix from the path of a store, having no effect.") {
    val path = "a/b/c/myfile.txt"
    val suffix = ".gz"
    val store = Store.create.at(path).asInput
    val storeRef = store - suffix
    assert(storeRef.path(fileManager).toString === store.pathOpt.get.toString)
  }
}
