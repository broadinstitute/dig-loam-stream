package loamstream.apps

import scala.io.Source

import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite

import loamstream.util.Files
import loamstream.util.LoamFileUtils

import loamstream.TestHelpers
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamProject
import loamstream.compiler.LoamEngine
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.loam.LoamScript
import java.nio.file.Path
import loamstream.util.PathEnrichments

/**
 * @author clint
 * Jul 11, 2017
 */
final class LoamRunnerTest extends FunSuite {
  import TestHelpers.path
  import PathEnrichments._
  
  test("dynamic execution") {
    val workDir = path("target/MainTest")
    
    doTest(
      workDir, 
      workDir / "storeFinal.txt",
      "This is line 1 This is line 2 This is line 3",
      Code.oneAndThen(workDir))
  }
  
  test("dynamic execution - multiple top-level andThens") {
    val workDir = path("target/MainTest2")
    
    doTest(
      workDir,
      workDir / "storeFinal.txt",
      "line 1 line 2 line 3 line 4 line 5 line 6",
      Code.twoAndThens(workDir))
  }
  
  private def doTest(dir: Path, finalOutputFile: Path, expectedContents: String, code: String): Unit = {
    nukeAndRemake(dir)
    
    import TestHelpers.config
    
    val loamEngine: LoamEngine = LoamEngine.default(config)
    
    @volatile var timesShutdown: Int = 0
    @volatile var timesCompiled: Int = 0
    
    def noopShutdown[A]: ( => A) => A = incAfter(timesShutdown += 1) { f => f }
    
    val compile: LoamProject => LoamCompiler.Result = incAfter(timesCompiled += 1)(loamEngine.compile)
   
    val loamRunner = LoamRunner(loamEngine, compile, noopShutdown)

    val project = LoamProject(config, LoamScript.withGeneratedName(code))
    
    assert(timesShutdown === 0)
    assert(timesCompiled === 0)
    
    val results = loamRunner.run(project)
    
    assert(timesShutdown === 1)
    assert(timesCompiled === 1)
    
    assert(results.nonEmpty)
    
    val contents = LoamFileUtils.enclosed(Source.fromFile(finalOutputFile.toFile)) { source =>
      source.getLines.map(_.trim).filter(_.nonEmpty).mkString(" ")
    }
    
    assert(contents === expectedContents)
    
    assert(results.values.forall(_.isSuccess))
  }
  
  private def nukeAndRemake(dir: Path): Unit = {
    FileUtils.deleteDirectory(dir.toFile)
    
    assert(dir.toFile.exists === false)
    
    dir.toFile.mkdir()
    
    assert(dir.toFile.exists === true)
  }
  
  private def incAfter[A, B](incOp: => Unit)(f: A => B): A => B = { a =>
    try { f(a) } finally { incOp }
  }
  
  private object Code {
    def oneAndThen(dir: Path): String = """
import scala.collection.mutable.{Buffer, ArrayBuffer}
import loamstream.model.Store
import loamstream.util.Files

val workDir = path("""" + dir + """")

val storeInitial = store[TXT].at(workDir / "storeInitial.txt")
val storeFinal = store[TXT].at(workDir / "storeFinal.txt")

def createStore(i: Int): Store[TXT] = store[TXT].at(workDir / s"store$i.txt")

cmd"printf 'line1\nline2\nline3\n' > $storeInitial".out(storeInitial)

andThen {
  val numLines = Files.countLines(storeInitial.path).toInt

  val stores: Buffer[Store[TXT]] = new ArrayBuffer

  for (i <- 1 to numLines) {
    val newStore = createStore(i)
    stores += newStore
    cmd"printf 'This is line $i\n' > $newStore".in(storeInitial).out(newStore)
  }
  
  cmd"cat ${workDir}/store?.txt > ${storeFinal}".in(stores).out(storeFinal)
}
"""

    def twoAndThens(dir: Path) = """
import scala.collection.mutable.{Buffer, ArrayBuffer}
import loamstream.model.Store
import loamstream.util.Files

val workDir = path("""" + dir + """")

val storeInitial = store[TXT].at(workDir / "storeInitial.txt")
val storeMiddle = store[TXT].at(workDir / "storeMiddle.txt")
val storeFinal = store[TXT].at(workDir / "storeFinal.txt")

cmd"printf 'line1\nline2\nline3\n' > $storeInitial".out(storeInitial)

andThen {
  val numLines = Files.countLines(storeInitial.path).toInt

  val stores: Buffer[Store[TXT]] = new ArrayBuffer

  for (i <- 1 to numLines) {
    val newStore = store[TXT].at(workDir / s"mid-$i.txt")
    stores += newStore
    cmd"printf 'This is line $i\n' > $newStore".in(storeInitial).out(newStore)
  }
  
  cmd"cat ${workDir}/mid-?.txt > ${storeMiddle} && cat ${workDir}/mid-?.txt >> ${storeMiddle}"
  .in(stores).out(storeMiddle)
}

andThen {
  val numLines = Files.countLines(storeMiddle.path).toInt
  
  val stores: Buffer[Store[TXT]] = new ArrayBuffer

  for (i <- 1 to numLines) {
    val newStore = store[TXT].at(workDir / s"store-$i.txt")
    stores += newStore
    cmd"printf 'line $i\n' > $newStore".in(storeInitial).out(newStore)
  }
  
  cmd"cat ${workDir}/store-?.txt > ${storeFinal}".in(stores).out(storeFinal)
}
"""

  }
}