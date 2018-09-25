package loamstream.apps

import java.nio.file.Files.exists
import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.IntegrationTestHelpers
import loamstream.cli.Intent
import loamstream.cli.JobFilterIntent
import loamstream.db.LoamDao
import loamstream.db.slick.DbDescriptor
import loamstream.drm.DrmSystem
import loamstream.model.execute.HashingStrategy
import loamstream.util.Files
import loamstream.model.execute.EnvironmentType

/**
 * @author clint
 * Jan 29, 2018
 */
final class SimplePipelineTest extends FunSuite {
    
  import loamstream.cli.JobFilterIntent.{ DontFilterByName, RunEverything }
  import HashingStrategy.{ HashOutputs, DontHashOutputs }
  import SimplePipelineTest.EnvironmentDescriptor
  import SimplePipelineTest.EnvironmentDescriptor._
  
  test(s"A simple linear pipeline running locally should work") {
    doTest(Local, jobFilterIntent = RunEverything, hashingStrategy = HashOutputs)
    doTest(Local, jobFilterIntent = DontFilterByName, hashingStrategy = HashOutputs)
    doTest(Local, jobFilterIntent = RunEverything, hashingStrategy = DontHashOutputs)
    doTest(Local, jobFilterIntent = DontFilterByName, hashingStrategy = DontHashOutputs)
  }
   
  test(s"A simple linear pipeline running on Uger should work") {
    //NB: Due to DRMAA limitations, tests requiring UGER and running in the same JVM must run sequentially, 
    //NOT concurrently!  Concurrent test runs using Uger can happen in different JVM processes.
    doTest(Uger, jobFilterIntent = RunEverything, hashingStrategy = HashOutputs)
    doTest(Uger, jobFilterIntent = DontFilterByName, hashingStrategy = HashOutputs)
    doTest(Uger, jobFilterIntent = RunEverything, hashingStrategy = DontHashOutputs)
    doTest(Uger, jobFilterIntent = DontFilterByName, hashingStrategy = DontHashOutputs)
    
    val image = "docker://library/ubuntu:18.04"
    
    doTest(UgerInContainer(image), jobFilterIntent = RunEverything, hashingStrategy = HashOutputs)
    doTest(UgerInContainer(image), jobFilterIntent = DontFilterByName, hashingStrategy = HashOutputs)
    doTest(UgerInContainer(image), jobFilterIntent = RunEverything, hashingStrategy = DontHashOutputs)
    doTest(UgerInContainer(image), jobFilterIntent = DontFilterByName, hashingStrategy = DontHashOutputs)
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
                       |    singularity {
                       |      mappedDirs = ["/humgen"]
                       |    }
                       |  }
                       |  ${ugerSection}
                       |}""".stripMargin

    Files.writeTo(configFilePath)(contents)
    
    assert(Files.readFrom(configFilePath) === contents)
  }
  
  private def doTest(
      environmentDescriptor: EnvironmentDescriptor, 
      jobFilterIntent: JobFilterIntent, 
      hashingStrategy: HashingStrategy): Unit = {
    
    import loamstream.util.Paths.Implicits._
    
    val testTag = s"${environmentDescriptor.toFileNamePart}-${jobFilterIntent}-${hashingStrategy}"
    
    val workDir = IntegrationTestHelpers.getWorkDirUnderTarget(Some(testTag))
    
    assert(exists(workDir) === true)
    
    val pathA = workDir / "a.txt"
    val pathB = workDir / "b.txt"
    val pathC = workDir / "c.txt"
    
    val confFilePath = workDir / "loamstream.conf"
    
    val drmWorkDir = if(environmentDescriptor.isUger) Some(workDir / "uger") else None
    
    drmWorkDir.foreach { wd =>
      assert(wd.toFile.mkdirs() === true)
    }
    
    val jobOutputDir = workDir / "job-outputs"
    
    assert(jobOutputDir.toFile.mkdirs() === true)
    
    writeConfFileTo(confFilePath, drmWorkDir, jobOutputDir)
    
    Files.writeTo(pathA)("AAA")
    
    assert(Files.readFrom(pathA) === "AAA")
    
    assert(exists(pathA))
    assert(exists(pathB) === false)
    assert(exists(pathC) === false)
    
    val envFnName = environmentDescriptor.toLoamCode
    
    val loamScriptContents = {
      s"""|
          |val a = store("${pathA}").asInput
          |val b = store("${pathB}")
          |val c = store("${pathC}")
          |
          |${envFnName} { //should be 'local', 'drm', or 'drmWith(imageName = ...)'
          |  cmd"cp $$b $$c".in(b).out(c) //NB: declare commands "out of order"
          |
          |  cmd"cp $$a $$b".in(a).out(b)
          |}
          |""".stripMargin.trim
    }
    
    val loamScriptPath = workDir / s"copy-a-b-c-${testTag}.loam"
    
    Files.writeTo(loamScriptPath)(loamScriptContents)
    
    assert(Files.readFrom(loamScriptPath) === loamScriptContents)

    val intent = Intent.RealRun(
      confFile = Some(confFilePath),
      hashingStrategy = hashingStrategy,
      jobFilterIntent = jobFilterIntent,
      drmSystemOpt = environmentDescriptor.drmSystem,
      loams = Seq(loamScriptPath))
    
    (new Main.Run).doRealRun(intent, dao)
    
    assert(exists(pathA))
    assert(exists(pathB))
    assert(exists(pathC))
    
    assert(Files.readFrom(pathA) === Files.readFrom(pathB))
    assert(Files.readFrom(pathB) === Files.readFrom(pathC))
  }
}

object SimplePipelineTest {
  private sealed abstract class EnvironmentDescriptor {
    def toFileNamePart: String
    
    def toLoamCode: String = toFileNamePart
    
    import EnvironmentDescriptor._
    
    def isUger: Boolean = this match {
      case Uger | UgerInContainer(_) => true
      case _ => false
    }
    
    def drmSystem: Option[DrmSystem] = this match {
      case Uger | UgerInContainer(_) => Some(DrmSystem.Uger)
      case _ => None
    }
  }
  
  private object EnvironmentDescriptor {
    final case object Local extends EnvironmentDescriptor {
      override def toFileNamePart: String = "local"
    }
    
    final case object Uger extends EnvironmentDescriptor  {
      override def toFileNamePart: String = "drm"
    }
    
    final case class UgerInContainer(imageName: String) extends EnvironmentDescriptor  {
      override def toFileNamePart: String = {
        def munge(s: String): String = s.map { 
          case ':' | '/' | '.' => '_'
          case c => c
        }
        
        s"drm-${munge(imageName)}"
      }
      
      override def toLoamCode: String = s"""drmWith(imageName = "${imageName}")"""
    }

    //Someday: LSF?
  }
}
