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
import loamstream.conf.LsSettings

/**
 * @author clint
 * Jul 11, 2017
 */
final class LoamRunnerTest extends FunSuite {
  import TestHelpers.path
  import Paths.Implicits._

  test("run some loam code") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    doTest(
      workDir,
      workDir / "storeFinal.txt",
      "foo",
      Code.simplePipeline(workDir))
  }

  private def doTest(dir: Path, finalOutputFile: Path, expectedContents: String, code: String): Unit = {

    import TestHelpers.config

    val loamEngine: LoamEngine = TestHelpers.loamEngine
    
    val loamRunner = LoamRunner(loamEngine)

    val project = LoamProject(config, LsSettings.noCliConfig, LoamScript.withGeneratedName(code))
    
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

    def simplePipeline(dir: Path): String = {
      s"""|
          |val workDir = path("${dir.render}")
          |
          |val storeInitial = store(workDir / "storeInitial.txt")
          |val storeFinal = store(workDir / "storeFinal.txt")
          |
          |cmd"echo 'foo' > $$storeInitial".out(storeInitial)
          |
          |cmd"cp $$storeInitial $$storeFinal".in(storeInitial).out(storeFinal)
          |""".stripMargin
    }
  }
}
