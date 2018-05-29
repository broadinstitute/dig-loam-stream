package loamstream.cli

import java.net.URI
import java.nio.file.Path

import org.scalatest.FunSuite

import Intent.CompileOnly
import Intent.DryRun
import Intent.LookupOutput
import Intent.RealRun
import Intent.ShowVersionAndQuit
import Intent.from
import loamstream.TestHelpers
import loamstream.TestHelpers.path
import loamstream.model.execute.HashingStrategy
import loamstream.drm.DrmSystem

/**
 * @author clint
 * Dec 13, 2017
 */
final class IntentTest extends FunSuite {
  //These need to exist
  private val exampleFile0 = "src/examples/loam/cp.loam"
  private val exampleFile1 = "src/examples/loam/first.loam"
  
  private val nonExistentFile = "/foo/bar/baz.loam"
  
  private val confFile = "src/test/resources/foo.conf"
  private val confPath = TestHelpers.path(confFile)
  
  private val outputFile = "src/test/resources/a.txt"
  private val outputUri = "gs://foo/bar/baz"

  private def cliConf(argString: String): Conf = Conf(argString.split("\\s+").toSeq)

  private def uri(u: String): URI = URI.create(u)
    
  private def paths(ps: String*): Seq[Path] = ps.map(TestHelpers.path)
    
  private def assertInvalid(commandLine: String): Unit = {
    assert(from(cliConf(commandLine)).isLeft)
  }
    
  private def assertValid(commandLine: String, expected: Intent): Unit = {
    assert(from(cliConf(commandLine)).right.get === expected)
  }
  
  test("from") {
    //Junk params
    assertInvalid("asdfasdfasdf")
    
    //missing conf file
    assertInvalid(s"--conf $nonExistentFile")
    
    //--version
    assertValid("--version", ShowVersionAndQuit)
    assertValid("-v", ShowVersionAndQuit)
    assertValid("--version --compile-only", ShowVersionAndQuit)
    assertValid("--version --conf blah.conf foo.loam bar.loam", ShowVersionAndQuit)
    
    //--compile-only
    assertInvalid("--compile-only") //no loams specified
    
    assertValid(s"--compile-only $exampleFile0 $exampleFile1", CompileOnly(None, paths(exampleFile0, exampleFile1)))
    
    assertValid(
        s"--conf $confFile --compile-only $exampleFile0 $exampleFile1", 
        CompileOnly(Some(confPath), paths(exampleFile0, exampleFile1)))
    
    assertInvalid(s"--conf $confFile --compile-only $exampleFile0 $nonExistentFile") //nonexistent loam file
    
    assertInvalid(s"--compile-only $exampleFile0 $exampleFile1 --conf $confFile") //loams must come at the end
    
    //--dry-run
    assertInvalid("--dry-run") //no loams specified
    
    assertValid(
        s"--dry-run $exampleFile0 $exampleFile1", 
        DryRun(
          confFile = None,
          hashingStrategy = HashingStrategy.HashOutputs,
          shouldRunEverything = false,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertValid(
        s"--conf $confFile --dry-run $exampleFile0 $exampleFile1", 
        DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.HashOutputs,
          shouldRunEverything = false,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertValid(
        s"--conf $confFile --dry-run --run-everything $exampleFile0 $exampleFile1", 
        DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.HashOutputs,
          shouldRunEverything = true,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertValid(
        s"--conf $confFile --dry-run --disable-hashing $exampleFile0 $exampleFile1", 
        DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.DontHashOutputs,
          shouldRunEverything = false,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertValid(
        s"--conf $confFile --dry-run --run-everything --disable-hashing $exampleFile0 $exampleFile1", 
        DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.DontHashOutputs,
          shouldRunEverything = true,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertInvalid(s"--conf $confFile --dry-run $exampleFile0 $nonExistentFile") //nonexistent loam file
    
    assertInvalid(s"--dry-run $exampleFile0 $exampleFile1 --conf $confFile") //loams must come at the end
    
    //--lookup
    assertInvalid("--lookup") //no output file
    assertInvalid(s"--conf $confFile --lookup") //no output file
    
    assertValid(s"--lookup $outputFile", LookupOutput(None, Left(path(outputFile))))
    assertValid(s"--lookup $outputUri", LookupOutput(None, Right(uri(outputUri))))
    
    assertValid(s"--conf $confFile --lookup $outputFile", LookupOutput(Some(confPath), Left(path(outputFile))))
    assertValid(s"--conf $confFile --lookup $outputUri", LookupOutput(Some(confPath), Right(uri(outputUri))))
    
    //Run some loams
    
    assertInvalid(s"$exampleFile0 $nonExistentFile")
    
    assertValid(
        s"$exampleFile0 $exampleFile1", 
        RealRun(
            confFile = None,
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = false,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = false,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--uger --conf $confFile $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = false,
            drmSystemOpt = Some(DrmSystem.Uger),
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--lsf --conf $confFile $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = false,
            drmSystemOpt = Some(DrmSystem.Lsf),
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--run-everything $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = None,
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = true,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile --run-everything $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = true,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--run-everything --disable-hashing $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = None,
            hashingStrategy = HashingStrategy.DontHashOutputs,
            shouldRunEverything = true,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile --run-everything --disable-hashing $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.DontHashOutputs,
            shouldRunEverything = true,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--lsf --conf $confFile --run-everything --disable-hashing $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.DontHashOutputs,
            shouldRunEverything = true,
            drmSystemOpt = Some(DrmSystem.Lsf),
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--uger --conf $confFile --run-everything --disable-hashing $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.DontHashOutputs,
            shouldRunEverything = true,
            drmSystemOpt = Some(DrmSystem.Uger),
            loams = paths(exampleFile0, exampleFile1)))
  }

  test("determineHashingStrategy") {
    {
      val conf = cliConf(s"--disable-hashing --compile-only $exampleFile0")

      assert(Intent.determineHashingStrategy(conf.toValues) === HashingStrategy.DontHashOutputs)
    }

    {
      val conf = cliConf(s"--compile-only $exampleFile0")

      assert(Intent.determineHashingStrategy(conf.toValues) === HashingStrategy.HashOutputs)
    }
  }
}
