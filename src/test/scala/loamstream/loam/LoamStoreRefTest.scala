package loamstream.loam

import loamstream.compiler.LoamPredef.VCF
import loamstream.loam.files.LoamFileManager
import loamstream.util.ValueBox
import org.scalatest.FunSuite

/** Tests of LoamStoreRef, basically whether the path comes out right */
final class LoamStoreRefTest extends FunSuite {
  implicit val graphBox = new ValueBox(LoamGraph.empty)
  val fileManager = new LoamFileManager
  test("Adding a suffix to the path of a store.") {
    val path = "a/b/c/myfile.txt"
    val suffix = ".gz"
    val store = LoamStore.create[VCF].from(path)
    val storeRef = store + suffix
    assert(storeRef.path(fileManager).toString === store.pathOpt.get.toString + suffix)
  }
  test("Removing a suffix from the path of a store.") {
    val path = "a/b/c/myfile.txt"
    val suffix = ".txt"
    val store = LoamStore.create[VCF].from(path)
    val storeRef = store - suffix
    assert(storeRef.path(fileManager).toString + suffix === store.pathOpt.get.toString)
  }
  test("Try removing a non-existing suffix from the path of a store, having no effect.") {
    val path = "a/b/c/myfile.txt"
    val suffix = ".gz"
    val store = LoamStore.create[VCF].from(path)
    val storeRef = store - suffix
    assert(storeRef.path(fileManager).toString === store.pathOpt.get.toString)
  }
}
