package loamstream.compiler.repo

import loamstream.loam.LoamScript
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Jul 20, 2016
  */
final class LoamMapRepositoryTest extends FunSuite {
  test("list") {
    val empty = LoamMapRepository(Nil)

    assert(empty.list == Nil)

    val repo = LoamMapRepository(Seq(LoamScript("foo", "fooContent"), LoamScript("bar", "barContent")))

    assert(repo.list.toSet == Set("foo", "bar"))
  }

  test("load") {
    val repo = LoamMapRepository(Seq(LoamScript("foo", "fooContent"), LoamScript("bar", "barContent")))

    assert(repo.load("foo").get.name == "foo")
    assert(repo.load("foo").get.code == "fooContent")

    assert(repo.load("bar").get.name == "bar")
    assert(repo.load("bar").get.code == "barContent")

    assert(repo.load("baz").isMiss)
  }

  test("save") {
    val repo = LoamMapRepository(Nil)

    val script = repo.save(LoamScript("foo", "fooContent")).get

    assert(script.name == "foo")

    assert(repo.entries("foo").code == "fooContent")
  }
}