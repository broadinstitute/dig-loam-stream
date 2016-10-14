package loamstream.compiler.repo

import org.scalatest.FunSuite
import loamstream.util.Files
import java.nio.file.Paths

/**
 * @author clint
 * date: Jul 20, 2016
 */
final class LoamPackageRepositoryTest extends FunSuite {
  
  private val noEntries = LoamPackageRepository("loam", Nil)
  private val repo = LoamPackageRepository("loam", Seq("impute", "singletons-via-hail"))
  
  test("list") {
    assert(noEntries.list == Nil)
    
    assert(repo.list.toSet == Set("impute", "singletons-via-hail"))
  }
  
  test("load") {
    assert(noEntries.load("foo").isMiss)
    assert(repo.load("foo").isMiss)
    
    assert(repo.load("impute").get.name == "impute")
    assert(repo.load("impute").get.code == Files.readFromAsUtf8(Paths.get("src/test/resources/loam/impute.loam")))
    
    assert(repo.load("foo").isMiss)
  }
}