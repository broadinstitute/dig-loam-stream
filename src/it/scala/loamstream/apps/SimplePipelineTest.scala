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
import loamstream.drm.DrmSystem
import loamstream.cli.JobFilterIntent

/**
 * @author clint
 * Jan 29, 2018
 */
final class SimplePipelineTest extends FunSuite with IntegrationTestHelpers {
    
  import JobFilterIntent.{RunEverything, DontFilterByName}
  
  test(s"A simple linear pipeline running locally should work") {
    doTest("local", jobFilterIntent = RunEverything, hashingStrategy = HashingStrategy.HashOutputs)
    doTest("local", jobFilterIntent = DontFilterByName, hashingStrategy = HashingStrategy.HashOutputs)
    doTest("local", jobFilterIntent = RunEverything, hashingStrategy = HashingStrategy.DontHashOutputs)
    doTest("local", jobFilterIntent = DontFilterByName, hashingStrategy = HashingStrategy.DontHashOutputs)
  }
   
  test(s"A simple linear pipeline running on Uger should work") {
    //NB: Due to DRMAA limitations, tests requiring UGER and running in the same JVM must run sequentially, 
    //NOT concurrently!  Concurrent test runs using Uger can happen in different JVM processes.
    doTest("uger", jobFilterIntent = RunEverything, hashingStrategy = HashingStrategy.HashOutputs)
    doTest("uger", jobFilterIntent = DontFilterByName, hashingStrategy = HashingStrategy.HashOutputs)
    doTest("uger", jobFilterIntent = RunEverything, hashingStrategy = HashingStrategy.DontHashOutputs)
    doTest("uger", jobFilterIntent = DontFilterByName, hashingStrategy = HashingStrategy.DontHashOutputs)
  }
  
  private val dbDescriptor: DbDescriptor = IntegrationTestHelpers.inMemoryH2(this.getClass.getSimpleName)
      
  private val dao: LoamDao = AppWiring.makeDaoFrom(dbDescriptor)
  
  private def writeConfFileTo(configFilePath: Path, ugerWorkDir: Option[Path], jobOutputDir: Path): Unit = {
    //Don't create a Uger section if we're not going to run anything on Uger, which will prevent DRMAA resources
    //from being acquired if they're not needed.
    val ugerSection = ugerWorkDir match {
      case Some(dir) => s"""|  uger {
                            |    maxNumJobs = 2400
                            |    workDir = "$dir"
                            |  }""".stripMargin
      case None => ""
    }
    
    val contents = s"""|loamstream {
                       |  execution {
                       |    outputDir = "$jobOutputDir"
                       |  }
                       |  ${ugerSection}
                       |}""".stripMargin

    Files.writeTo(configFilePath)(contents)
  }
  
  private def doTest(environment: String, jobFilterIntent: JobFilterIntent, hashingStrategy: HashingStrategy): Unit = {
    
    import PathEnrichments._
    
    val workDir = getWorkDirUnderTarget(Some(s"$environment-$jobFilterIntent-$hashingStrategy"))
    
    assert(exists(workDir) === true)
    
    val pathA = workDir / "a.txt"
    val pathB = workDir / "b.txt"
    val pathC = workDir / "c.txt"
    
    val confFilePath = workDir / "loamstream.conf"
    
    val ugerWorkDir = if(environment == "uger") Some(workDir / "uger") else None
    
    ugerWorkDir.foreach { uwd =>
      assert(uwd.toFile.mkdirs() === true)
    }
    
    val jobOutputDir = workDir / "job-outputs"
    
    assert(jobOutputDir.toFile.mkdirs() === true)
    
    writeConfFileTo(confFilePath, ugerWorkDir, jobOutputDir)
    
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
    
    val loamScriptPath = workDir / s"copy-a-b-c-$environment-$jobFilterIntent-$hashingStrategy.loam"
    
    Files.writeTo(loamScriptPath)(loamScriptContents)
    
    assert(Files.readFrom(loamScriptPath) === loamScriptContents)

    val drmSystemOpt: Option[DrmSystem] = environment.toLowerCase match {
      case "uger" => Some(DrmSystem.Uger)
      case "lsf" => Some(DrmSystem.Lsf)
      case _ => None
    }
    
    val intent = Intent.RealRun(
      confFile = Some(confFilePath),
      hashingStrategy = hashingStrategy,
      jobFilterIntent = jobFilterIntent,
      drmSystemOpt = drmSystemOpt,
      loams = Seq(loamScriptPath))
    
    (new Main.Run).doRealRun(intent, dao)
    
    assert(exists(pathA))
    assert(exists(pathB))
    assert(exists(pathC))
    
    assert(Files.readFrom(pathA) === Files.readFrom(pathB))
    assert(Files.readFrom(pathB) === Files.readFrom(pathC))
  }
}
