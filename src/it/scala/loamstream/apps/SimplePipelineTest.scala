package loamstream.apps

import org.scalatest.FunSuite
import loamstream.util.Files
import java.nio.file.Files.exists
import loamstream.IntegrationTestHelpers
import loamstream.cli.Intent
import loamstream.util.PathEnrichments
import loamstream.model.execute.HashingStrategy
import java.nio.file.Path
import loamstream.db.slick.DbDescriptor
import loamstream.db.LoamDao
import java.nio.file.Paths

/**
 * @author clint
 * Jan 29, 2018
 */
final class SimplePipelineTest extends FunSuite with IntegrationTestHelpers {
    
  test(s"A simple linear pipeline running locally should work") {
    doTest("local", shouldRunEverything = true, hashingStrategy = HashingStrategy.HashOutputs)
    doTest("local", shouldRunEverything = false, hashingStrategy = HashingStrategy.HashOutputs)
    doTest("local", shouldRunEverything = true, hashingStrategy = HashingStrategy.DontHashOutputs)
    doTest("local", shouldRunEverything = false, hashingStrategy = HashingStrategy.DontHashOutputs)
  }
   
  test(s"A simple linear pipeline running on Uger should work") {
    doTest("uger", shouldRunEverything = true, hashingStrategy = HashingStrategy.HashOutputs)
    doTest("uger", shouldRunEverything = false, hashingStrategy = HashingStrategy.HashOutputs)
    doTest("uger", shouldRunEverything = true, hashingStrategy = HashingStrategy.DontHashOutputs)
    doTest("uger", shouldRunEverything = false, hashingStrategy = HashingStrategy.DontHashOutputs)
  }
  
  private val dbDescriptor: DbDescriptor = IntegrationTestHelpers.inMemoryH2(this.getClass.getSimpleName)
      
  private val dao: LoamDao = AppWiring.makeDaoFrom(dbDescriptor)
  
  private def writeConfFileTo(configFilePath: Path, ugerWorkDir: Path): Unit = {
    val contents = s"""|loamstream {
                       |  uger {
                       |    maxNumJobs = 2400
                       |    workDir = "$ugerWorkDir"
                       |  }
                       |}""".stripMargin

    Files.writeTo(configFilePath)(contents)
  }
  
  private def doTest(environment: String, shouldRunEverything: Boolean, hashingStrategy: HashingStrategy): Unit = {
    val tempDir = getWorkDir
    
    import PathEnrichments._
    
    val pathA = tempDir / "a.txt"
    val pathB = tempDir / "b.txt"
    val pathC = tempDir / "c.txt"
    
    val confFilePath = tempDir / "loamstream.conf"
    
    val ugerWorkDir = tempDir / "uger"
    
    writeConfFileTo(confFilePath, ugerWorkDir)
    
    assert(ugerWorkDir.toFile.mkdir())
    
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
          |$environment { //should be 'local' or 'uger'
          |  cmd"cp $$b $$c".in(b).out(c) //NB: declare commands "out of order"
          |
          |  cmd"cp $$a $$b".in(a).out(b)
          |}
          |""".stripMargin.trim
    }
    
    val loamScriptPath = tempDir / s"copy-a-b-c-$environment-$shouldRunEverything-$hashingStrategy.loam"
    
    Files.writeTo(loamScriptPath)(loamScriptContents)
    
    assert(Files.readFrom(loamScriptPath) === loamScriptContents)

    val intent = Intent.RealRun(
      confFile = Some(confFilePath),
      hashingStrategy = hashingStrategy,
      shouldRunEverything = shouldRunEverything,
      loams = Seq(loamScriptPath))
    
    Main.doRealRun(intent, dao)
    
    assert(exists(pathA))
    assert(exists(pathB))
    assert(exists(pathC))
    
    assert(Files.readFrom(pathA) === Files.readFrom(pathB))
    assert(Files.readFrom(pathB) === Files.readFrom(pathC))
  }
}
