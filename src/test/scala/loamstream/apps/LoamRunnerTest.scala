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

/**
 * @author clint
 * Jul 11, 2017
 */
final class LoamRunnerTest extends FunSuite {
  test("dynamic execution") {
    import TestHelpers.path
    
    val workDir = path("target/MainTest")
    
    FileUtils.deleteDirectory(workDir.toFile)
    
    assert(workDir.toFile.exists === false)
    
    workDir.toFile.mkdir()
    
    assert(workDir.toFile.exists === true)
    
    val code = """
import scala.collection.mutable.{Buffer, ArrayBuffer}
import loamstream.model.Store
import loamstream.util.Files

val workDir = path("target/MainTest")

Files.createDirsIfNecessary(workDir)

val storeInitial = store[TXT].at(workDir / "storeInitial.txt")
val storeFinal = store[TXT].at(workDir / "storeFinal.txt")
val stores: Buffer[Store[TXT]] = new ArrayBuffer

def createStore(i: Int): Store[TXT] = store[TXT].at(workDir / s"store$i.txt")

cmd"printf 'line1\nline2\nline3\n' > $storeInitial".out(storeInitial)

andThen {
  val numLines = Files.countLines(storeInitial.path).toInt

  for (i <- 1 to numLines) {
    val newStore = createStore(i)
    stores += newStore
    cmd"printf 'This is line $i\n' > $newStore".in(storeInitial).out(newStore)
  }
  
  cmd"cat ${workDir}/store?.txt > ${storeFinal}".in(stores).out(storeFinal)
}
"""
    import TestHelpers.config
    
    val loamEngine: LoamEngine = LoamEngine.default(config)
    
    @volatile var timesShutdown: Int = 0
    @volatile var timesCompiled: Int = 0
    
    def incAfter[A, B](incOp: => Unit)(f: A => B): A => B = { a =>
      try { f(a) } finally { incOp }
    }
    
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
    
    val finalOutputFile = path("target/MainTest/storeFinal.txt")
    
    LoamFileUtils.enclosed(Source.fromFile(finalOutputFile.toFile)) { source =>
      val contents = {
        try { source.getLines.map(_.trim).filter(_.nonEmpty).mkString(" ") }
        finally { source.close() }
      }
      
      val expectedOutput = "This is line 1 This is line 2 This is line 3"
      
      assert(contents === expectedOutput)
    }
    
    assert(results.values.forall(_.isSuccess))
  }
}
