package loamstream.apps

import org.scalatest.FunSuite
import loamstream.util.Files
import java.nio.file.Files.exists
import loamstream.IntegrationTestHelpers
import loamstream.cli.Intent
import loamstream.util.PathEnrichments
import loamstream.model.execute.HashingStrategy
import java.nio.file.Path

/**
 * @author clint
 * Jan 29, 2018
 */
object SimpleLocalPipelineTest {
  final case class Params(shouldRunEverything: Boolean, hashingStrategy: HashingStrategy) {
    def toIntent(loams: Path*): Intent.RealRun = Intent.RealRun(
      confFile = None,
      hashingStrategy = hashingStrategy,
      shouldRunEverything = shouldRunEverything,
      loams = loams)
  }
}

final class SimpleLocalPipelineTest extends FunSuite with IntegrationTestHelpers {
  import SimpleLocalPipelineTest.Params
  
  test("A simple linear pipeline running locally should work") {
    def doTest(params: Params): Unit = {
      val tempDir = getWorkDir
      
      import PathEnrichments._
      
      val pathA = tempDir / "a.txt"
      val pathB = tempDir / "b.txt"
      val pathC = tempDir / "c.txt"
      
      Files.writeTo(pathA)("AAA")
      
      assert(Files.readFrom(pathA) === "AAA")
      
      assert(exists(pathA))
      assert(exists(pathB) === false)
      assert(exists(pathC) === false)
      
      val loamScriptContents = {
        s"""|
            |val a = store.at("${pathA}").asInput
            |val b = store.at("${pathB}")
            |val c = store.at("${pathC}")
            |
            |cmd"cp $$b $$c".in(b).out(c)
            |
            |cmd"cp $$a $$b".in(a).out(b)
            |""".stripMargin.trim
      }
      
      val loamScriptPath = tempDir / "copy-a-b-c.loam"
      
      Files.writeTo(loamScriptPath)(loamScriptContents)
      
      assert(Files.readFrom(loamScriptPath) === loamScriptContents)
  
      val intent = params.toIntent(loamScriptPath)
      
      val dbDescriptor = IntegrationTestHelpers.inMemoryH2(this.getClass.getSimpleName)
      
      val dao = AppWiring.makeDaoFrom(dbDescriptor)
      
      Main.doRealRun(intent, dao)
      
      assert(exists(pathA))
      assert(exists(pathB))
      assert(exists(pathC))
      
      assert(Files.readFrom(pathA) === Files.readFrom(pathB))
      assert(Files.readFrom(pathB) === Files.readFrom(pathC))
    }
    
    doTest(Params(shouldRunEverything = true, hashingStrategy = HashingStrategy.HashOutputs))
    doTest(Params(shouldRunEverything = false, hashingStrategy = HashingStrategy.HashOutputs))
    doTest(Params(shouldRunEverything = true, hashingStrategy = HashingStrategy.DontHashOutputs))
    doTest(Params(shouldRunEverything = false, hashingStrategy = HashingStrategy.DontHashOutputs))
  }
}
