package loamstream.compiler.repo

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Jul 20, 2016
 */
final class LoamMapRepositoryTest extends FunSuite {
  test("list") {
    val empty = LoamMapRepository(Map.empty)
    
    assert(empty.list == Nil)
    
    val repo = LoamMapRepository(Map("foo" -> "fooContent", "bar" -> "barContent"))
    
    assert(repo.list.toSet == Set("foo", "bar"))
  }
  
  test("load") {
    val repo = LoamMapRepository(Map("foo" -> "fooContent", "bar" -> "barContent"))
    
    assert(repo.load("foo").get.name == "foo")
    assert(repo.load("foo").get.content == "fooContent")
    
    assert(repo.load("bar").get.name == "bar")
    assert(repo.load("bar").get.content == "barContent")
    
    assert(repo.load("baz").isMiss)
  }
 
  test("save") {
    val repo = LoamMapRepository(Map.empty)
    
    val message = repo.save("foo", "fooContent").get
    
    assert(message.name == "foo")
    
    assert(repo.entries("foo") == "fooContent")
  }
}