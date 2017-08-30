package loamstream.compiler.repo

import java.nio.file.Paths

import loamstream.loam.LoamScript
import loamstream.util.{Files, PathEnrichments}
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Jul 20, 2016
  */
final class LoamFolderRepositoryTest extends FunSuite {
  test("list") {
    val repo = LoamFolderRepository(Paths.get("src/test/resources/loam"))

    //NB: Use Sets to ignore order
    assert(repo.list.toSet == Set("impute"))
  }

  test("load") {
    val repo = LoamFolderRepository(Paths.get("src/test/resources/loam"))

    assert(repo.load("impute").get.name == "impute")
    assert(repo.load("impute").get.code == Files.readFromAsUtf8(Paths.get("src/test/resources/loam/impute.loam")))

    assert(repo.load("foo").isMiss)
  }

  test("save") {
    val dir = Paths.get("target/loam")

    dir.toFile.mkdir()

    import PathEnrichments._

    val newFile = dir / "foo.loam"

    newFile.toFile.delete()

    assert(!newFile.toFile.exists)

    val repo = LoamFolderRepository(dir)

    assert(repo.list == Nil)

    val script = repo.save(LoamScript("foo", "bar")).get

    assert(Files.readFromAsUtf8(Paths.get("target/loam/foo.loam")) == "bar")

    assert(script.name == "foo")
  }
}