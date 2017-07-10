package loamstream.apps

import org.scalatest.FunSuite
import loamstream.conf.LoamConfig
import loamstream.db.LoamDao
import loamstream.model.execute.Executer
import loamstream.googlecloud.CloudStorageClient
import org.apache.commons.io.FileUtils
import loamstream.TestHelpers
import scala.io.Source
import loamstream.util.Files

/**
 * @author clint
 * Mar 31, 2017
 */
final class MainTest extends FunSuite {
  test("shutdown") {
    val mockWiring = new MainTest.MockAppWiring
    
    assert(mockWiring.shutdownInvocations === 0)
    
    Main.shutdown(mockWiring)
    
    assert(mockWiring.shutdownInvocations === 1)
    
    Main.shutdown(mockWiring)
    
    assert(mockWiring.shutdownInvocations === 1)
    
    Main.shutdown(mockWiring)
    
    assert(mockWiring.shutdownInvocations === 1)
  }

  test("dynamic execution") {
    import TestHelpers.path
    
    val workDir = path("target/MainTest")
    
    FileUtils.deleteDirectory(workDir.toFile)
    
    assert(workDir.toFile.exists === false)
    
    workDir.toFile.mkdir()
    
    assert(workDir.toFile.exists === true)
    
    val resources = "src/test/resources"
    val loams = "src/main/loam"
    val conf = s"$resources/loamstream-test.conf"
    val loam = s"$workDir/dynamicExecution.loam"    
    
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

    val args = Array("--conf", conf, loam)    
    
    Files.writeTo(path(loam))(code)

    assert(path(loam).toFile.exists)
    
    Main.main(args)
    
    val finalOutputFile = path("target/MainTest/storeFinal.txt")
    
    val source = Source.fromFile(finalOutputFile.toFile)
    
    val contents = {
      try { source.getLines.map(_.trim).filter(_.nonEmpty).mkString(" ") }
      finally { source.close() }
    }
    
    val expectedOutput = "This is line 1 This is line 2 This is line 3"
    
    assert(contents === expectedOutput)
  }
}

object MainTest {
  final class MockAppWiring extends AppWiring {
    var shutdownInvocations: Int = 0
    
    override def config: LoamConfig = ???
  
    override def dao: LoamDao = ???

    override def executer: Executer = ???

    override def cloudStorageClient: Option[CloudStorageClient] = ???

    override def shutdown(): Seq[Throwable] = {
      shutdownInvocations += 1
      
      Nil
    }
  }
}
