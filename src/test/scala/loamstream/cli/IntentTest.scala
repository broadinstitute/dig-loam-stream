package loamstream.cli

import org.scalatest.FunSuite
import loamstream.model.execute.HashingStrategy
import loamstream.TestHelpers
import java.nio.file.Path
import java.net.URI
import TestHelpers.path
import Intent.{ from, ShowVersionAndQuit, CompileOnly, DryRun, LookupOutput, RealRun }

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
  private val confPath = path(confFile)
  
  private val outputFile = "src/test/resources/a.txt"
  private val outputUri = "gs://foo/bar/baz"

  private def cliConf(argString: String): Conf = Conf(argString.split("\\s+").toSeq)

  private def uri(u: String): URI = URI.create(u)
    
  private def paths(ps: String*): Seq[Path] = ps.map(path)
    
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
    
    assertValid(s"--dry-run $exampleFile0 $exampleFile1", DryRun(None, paths(exampleFile0, exampleFile1)))
    
    assertValid(
        s"--conf $confFile --dry-run $exampleFile0 $exampleFile1", 
        DryRun(Some(confPath), paths(exampleFile0, exampleFile1)))
    
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
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = false,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--run-everything $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = None,
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = true,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile --run-everything $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            shouldRunEverything = true,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--run-everything --disable-hashing $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = None,
            hashingStrategy = HashingStrategy.DontHashOutputs,
            shouldRunEverything = true,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile --run-everything --disable-hashing $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.DontHashOutputs,
            shouldRunEverything = true,
            loams = paths(exampleFile0, exampleFile1)))
  }

  test("determineHashingStrategy") {
    {
      val conf = cliConf(s"--disable-hashing --compile-only $exampleFile0")

      assert(Intent.determineHashingStrategy(conf) === HashingStrategy.DontHashOutputs)
    }

    {
      val conf = cliConf(s"--compile-only $exampleFile0")

      assert(Intent.determineHashingStrategy(conf) === HashingStrategy.HashOutputs)
    }
  }
}
