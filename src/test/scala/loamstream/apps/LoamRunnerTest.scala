package loamstream.apps

import scala.io.Source

import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite

import loamstream.util.CanBeClosed

import loamstream.TestHelpers
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamProject
import loamstream.compiler.LoamEngine
import loamstream.loam.LoamScript
import java.nio.file.Path
import loamstream.util.Paths
import loamstream.util.Files

/**
 * @author clint
 * Jul 11, 2017
 */
final class LoamRunnerTest extends FunSuite {
  import TestHelpers.path
  import Paths.Implicits._

  test("dynamic execution") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    doTest(
      workDir,
      workDir / "storeFinal.txt",
      "This is line 1 This is line 2 This is line 3",
      Code.oneAndThen(workDir))
  }

  test("dynamic execution - andThen throws") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    val code = Code.oneAndThenThatThrows(workDir)

    import TestHelpers.config

    val loamEngine: LoamEngine = TestHelpers.loamEngine
    
    val loamRunner = LoamRunner(loamEngine)

    val project = LoamProject(config, LoamScript.withGeneratedName(code))
    
    val thrown = intercept[Exception] {
      loamRunner.run(project)
    }

    assert(thrown.getMessage === "blerg")

    //assert that commands before the andThen ran
    val lastStoreWrittenTo = workDir / "storeInitial.txt"

    val expectedLines = Seq("line1", "line2", "line3")

    val actualLines = Files.getLines(lastStoreWrittenTo).map(_.trim)

    assert(actualLines === expectedLines)
  }

  test("dynamic execution - multiple top-level andThens") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    doTest(
      workDir,
      workDir / "storeFinal.txt",
      "line 1 line 2 line 3 line 4 line 5 line 6",
      Code.twoAndThens(workDir))
  }

  private def doTest(dir: Path, finalOutputFile: Path, expectedContents: String, code: String): Unit = {

    import TestHelpers.config

    val loamEngine: LoamEngine = TestHelpers.loamEngine
    
    val loamRunner = LoamRunner(loamEngine)

    val project = LoamProject(config, LoamScript.withGeneratedName(code))
    
    val results = loamRunner.run(project)
    
    val resultsToExecutions = results.right.toOption.get
    
    assert(resultsToExecutions.nonEmpty)
    
    val contents = CanBeClosed.enclosed(Source.fromFile(finalOutputFile.toFile)) { source =>
      source.getLines.map(_.trim).filter(_.nonEmpty).mkString(" ")
    }

    assert(contents === expectedContents)
    
    assert(resultsToExecutions.values.forall(_.isSuccess))
  }

  private object Code {
    import loamstream.util.BashScript.Implicits._

    // scalastyle:off line.size.limit
    def oneAndThen(dir: Path): String =
      s"""|import scala.collection.mutable.{Buffer, ArrayBuffer}
          |import loamstream.model.Store
          |import loamstream.util.Files
          |import loamstream.util.BashScript.Implicits._
          |
          |val workDir = path("${dir.render}")
          |
          |val storeInitial = store(workDir / "storeInitial.txt")
          |val storeFinal = store(workDir / "storeFinal.txt")
          |
          |def createStore(i: Int): Store = store(workDir / s"store$$i.txt")
          |
          |cmd"printf 'line1\\nline2\\nline3' > $$storeInitial".out(storeInitial)
          |
          |andThen {
          |  val numLines = Files.countLines(storeInitial.path).toInt
          |
          |  val stores: Buffer[Store] = new ArrayBuffer
          |
          |  for (i <- 1 to numLines) {
          |    val newStore = createStore(i)
          |    stores += newStore
          |    cmd"printf 'This is line $$i\\n' > $$newStore".in(storeInitial).out(newStore)
          |  }
          |
          |  cmd"cat $${workDir.render}/store?.txt > $${storeFinal}".in(stores).out(storeFinal)
          |}""".stripMargin

    def oneAndThenThatThrows(dir: Path): String =
      s"""|import scala.collection.mutable.{Buffer, ArrayBuffer}
          |import loamstream.model.Store
          |import loamstream.util.Files
          |import loamstream.util.BashScript.Implicits._
          |
          |val workDir = path("${dir.render}")
          |
          |val storeInitial = store(workDir / "storeInitial.txt")
          |val storeFinal = store(workDir / "storeFinal.txt")
          |
          |def createStore(i: Int): Store = store(workDir / s"store$$i.txt")
          |
          |cmd"printf 'line1\\nline2\\nline3' > $$storeInitial".out(storeInitial)
          |
          |andThen {
          |  throw new Exception("blerg")
          |}""".stripMargin

    def twoAndThens(dir: Path) =
      s"""|import scala.collection.mutable.{Buffer, ArrayBuffer}
          |import loamstream.model.Store
          |import loamstream.util.Files
          |import loamstream.util.BashScript.Implicits._
          |
          |val workDir = path("${dir.render}")
          |
          |val storeInitial = store(workDir / "storeInitial.txt")
          |val storeMiddle = store(workDir / "storeMiddle.txt")
          |val storeFinal = store(workDir / "storeFinal.txt")
          |
          |cmd"printf 'line1\\nline2\\nline3' > $$storeInitial".out(storeInitial)
          |
          |andThen {
          |  val numLines = Files.countLines(storeInitial.path).toInt
          |
          |  val stores: Buffer[Store] = new ArrayBuffer
          |
          |  for (i <- 1 to numLines) {
          |    val newStore = store(workDir / s"mid-$$i.txt")
          |    stores += newStore
          |    cmd"printf 'This is line $$i\\n' > $$newStore".in(storeInitial).out(newStore)
          |  }
          |
          |  cmd"cat $${workDir.render}/mid-?.txt > $${storeMiddle} && cat $${workDir.render}/mid-?.txt >> $${storeMiddle}"
          |  .in(stores).out(storeMiddle)
          |}
          |
          |andThen {
          |  val numLines = Files.countLines(storeMiddle.path).toInt
          |
          |  val stores: Buffer[Store] = new ArrayBuffer
          |
          |  for (i <- 1 to numLines) {
          |    val newStore = store(workDir / s"store-$$i.txt")
          |    stores += newStore
          |    cmd"printf 'line $$i\\n' > $$newStore".in(storeInitial).out(newStore)
          |  }
          |
          |  cmd"cat $${workDir.render}/store-?.txt > $${storeFinal}".in(stores).out(storeFinal)
          |}""".stripMargin
     // scalastyle:on line.size.limit
  }
}
