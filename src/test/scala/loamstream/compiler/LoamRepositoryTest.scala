package loamstream.compiler

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import loamstream.util.Files
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 6/27/2016.
  */
final class LoamRepositoryTest extends FunSuite {
  val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)

  def assertDefaultEntriesPresent(repo: LoamRepository): Unit = {
    for (entry <- LoamRepository.defaultEntries) {
      val codeShot = repo.load(entry)
      assert(codeShot.nonEmpty, codeShot.message)
    }
  }

  def assertAllEntriesCompile(repo: LoamRepository): Unit = {
    for (entry <- repo.list) {
      val codeShot = repo.load(entry).map(_.content)
      assert(codeShot.nonEmpty, codeShot.message)
      val code = codeShot.get
      val compileResult = compiler.compile(code)
      assert(compileResult.isSuccess, compileResult.report)
      assert(compileResult.isClean, compileResult.report)
    }
  }

  test("Memory repository works as expected") {
    val repo = LoamRepository.inMemory
    assert(repo.list.isEmpty)
    repo.save("yo", "Whatever")
    repo.save("hello", "How are you doing?")
    assert(repo.list.toSet === Set("yo", "hello"))
    val yoLoad = repo.load("yo")
    assert(yoLoad.nonEmpty)
    assert(yoLoad.get.name === "yo")
    assert(yoLoad.get.content === "Whatever")
    val helloLoad = repo.load("hello")
    assert(helloLoad.nonEmpty)
    assert(helloLoad.get.name === "hello")
    assert(helloLoad.get.content === "How are you doing?")
    assert(repo.load("nope").isEmpty)
  }
  test("Combo repository works as expected") {
    val repo1 = LoamRepository.ofMap(Map("1" -> "one", "2" -> "two"))
    val repo2 = LoamRepository.ofMap(Map("3" -> "three", "4" -> "four"))
    val combo = repo1 ++ repo2
    assert(combo.list.toSet === Set("1", "2", "3", "4"))
    val load1 = combo.load("1")
    assert(load1.nonEmpty)
    assert(load1.get.name === "1")
    assert(load1.get.content === "one")
    val load3 = combo.load("3")
    assert(load3.nonEmpty)
    assert(load3.get.name === "3")
    assert(load3.get.content === "three")
    assert(combo.load("5").isEmpty)
    combo.save("5", "five")
    val load5 = combo.load("5")
    assert(load5.nonEmpty)
    assert(load5.get.name === "5")
    assert(load5.get.content === "five")
    assert(combo.list.toSet === Set("1", "2", "3", "4", "5"))
    assert(repo1.list.toSet === Set("1", "2", "5"))
    assert(repo2.list.toSet === Set("3", "4"))
  }
  test("Folder repository works as expected"){
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
    assert(load1.get.content === "content of file1")
    repo.save("file3", "content of file3")
    assert(repo.list.toSet === Set("file1", "file2", "file3"))
    val load3 = repo.load("file3")
    assert(load3.nonEmpty)
    assert(load3.get.name === "file3")
    assert(load3.get.content === "content of file3")
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

