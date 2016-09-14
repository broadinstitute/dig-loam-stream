package loamstream.compiler

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import loamstream.loam.LoamScript
import loamstream.util.Files
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 6/27/2016.
  */
final class LoamRepositoryTest extends FunSuite {
  val compiler = new LoamCompiler(OutMessageSink.NoOp)

  def assertDefaultEntriesPresent(repo: LoamRepository): Unit = {
    for (entry <- LoamRepository.defaultEntries) {
      val scriptShot = repo.load(entry)
      assert(scriptShot.nonEmpty, scriptShot.message)
    }
  }

  def assertAllEntriesCompile(repo: LoamRepository): Unit = {
    for (entry <- repo.list) {
      val scriptShot = repo.load(entry)
      assert(scriptShot.nonEmpty, scriptShot.message)
      val script = scriptShot.get
      val compileResult = compiler.compile(script)
      assert(compileResult.isSuccess, script + "\n" + compileResult.report)
      assert(compileResult.isClean, script + "\n" + compileResult.report)
    }
  }

  test("Memory repository works as expected") {
    val repo = LoamRepository.inMemory
    assert(repo.list.isEmpty)
    repo.save(LoamScript("yo", "Whatever"))
    repo.save(LoamScript("hello", "How are you doing?"))
    assert(repo.list.toSet === Set("yo", "hello"))
    val yoLoad = repo.load("yo")
    assert(yoLoad.nonEmpty)
    assert(yoLoad.get.name === "yo")
    assert(yoLoad.get.code === "Whatever")
    val helloLoad = repo.load("hello")
    assert(helloLoad.nonEmpty)
    assert(helloLoad.get.name === "hello")
    assert(helloLoad.get.code === "How are you doing?")
    assert(repo.load("nope").isEmpty)
  }

  test("Combo repository works as expected") {
    val repo1 = LoamRepository.withScripts(Seq(LoamScript("1", "one"), LoamScript("2", "two")))
    val repo2 = LoamRepository.withScripts(Seq(LoamScript("3", "three"), LoamScript("4", "four")))
    val combo = repo1 ++ repo2
    assert(combo.list.toSet === Set("1", "2", "3", "4"))
    val load1 = combo.load("1")
    assert(load1.nonEmpty)
    assert(load1.get.name === "1")
    assert(load1.get.code === "one")
    val load3 = combo.load("3")
    assert(load3.nonEmpty)
    assert(load3.get.name === "3")
    assert(load3.get.code === "three")
    assert(combo.load("5").isEmpty)
    combo.save(LoamScript("5", "five"))
    val load5 = combo.load("5")
    assert(load5.nonEmpty)
    assert(load5.get.name === "5")
    assert(load5.get.code === "five")
    assert(combo.list.toSet === Set("1", "2", "3", "4", "5"))
    assert(repo1.list.toSet === Set("1", "2", "5"))
    assert(repo2.list.toSet === Set("3", "4"))
  }

  test("Folder repository works as expected") {
    val folder = java.nio.file.Files.createTempDirectory("repo")
    val file1 = folder.resolve("file1.loam")
    val file2 = folder.resolve("file2.loam")
    val fileShouldBeIgnored = folder.resolve("fileWithoutLoamSuffix.earth")
    Files.writeTo(file1)("content of file1")
    Files.writeTo(file2)("content of file2")
    Files.writeTo(fileShouldBeIgnored)("content of file that should be ignored")
    val repo = LoamRepository.ofFolder(folder)
    assert(repo.list.toSet === Set("file1", "file2"))
    val load1 = repo.load("file1")
    assert(load1.nonEmpty)
    assert(load1.get.name === "file1")
    assert(load1.get.code === "content of file1")
    repo.save(LoamScript("file3", "content of file3"))
    assert(repo.list.toSet === Set("file1", "file2", "file3"))
    val load3 = repo.load("file3")
    assert(load3.nonEmpty)
    assert(load3.get.name === "file3")
    assert(load3.get.code === "content of file3")
  }

  test("Default package repo contains all default entries") {
    assertDefaultEntriesPresent(LoamRepository.defaultPackageRepo)
  }

  test("Default repo contains all default entries") {
    assertDefaultEntriesPresent(LoamRepository.defaultRepo)
  }

  test("All entries of default package repo compile") {
    assertAllEntriesCompile(LoamRepository.defaultPackageRepo)
  }

  test("All entries of default repo compile") {
    assertAllEntriesCompile(LoamRepository.defaultRepo)
  }
}
