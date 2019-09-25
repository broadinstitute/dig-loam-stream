package loamstream.cli

import java.net.URI
import java.nio.file.Path

import org.scalatest.FunSuite

import Intent.CompileOnly
import Intent.DryRun
import Intent.RealRun
import Intent.ShowVersionAndQuit
import Intent.from
import loamstream.TestHelpers
import loamstream.TestHelpers.path
import loamstream.model.execute.HashingStrategy
import loamstream.drm.DrmSystem
import scala.util.Try
import scala.util.matching.Regex

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

  private def cliConf(commandLine: String): Either[String, Conf] = {
    Try(Conf(commandLine.split("\\s+"))).toEither.left.map(_.getMessage)
  }

  private def uri(u: String): URI = URI.create(u)
    
  private def paths(ps: String*): Seq[Path] = ps.map(TestHelpers.path)
    
  private def assertInvalid(commandLine: String): Unit = {
    assert(cliConf(commandLine).flatMap(from).isLeft)
  }
    
  private def assertValid(commandLine: String, expected: Intent): Unit = {
    val result = cliConf(commandLine).flatMap(from)
    
    assert(result.isRight, s"$result")
    assert(result.right.get === expected)
  }
  
  private def assertIsValidWithAllDrmSystems(
      baseCommandLine: String, 
      makeExpectedIntent: Option[DrmSystem] => Intent): Unit = {
    
    assertValid(baseCommandLine, makeExpectedIntent(None))
    assertValid(s"--backend uger ${baseCommandLine}", makeExpectedIntent(Some(DrmSystem.Uger)))
    assertValid(s"--backend lsf ${baseCommandLine}", makeExpectedIntent(Some(DrmSystem.Lsf)))
  }
  
  private def assertIsValidWithAllDrmSystemsAndValidationFlags(
      baseCommandLine: String, 
      makeExpectedIntent: (Boolean, Option[DrmSystem]) => Intent): Unit = {
    
    assertValid(baseCommandLine, makeExpectedIntent(true, None))
    assertValid(s"--backend uger ${baseCommandLine}", makeExpectedIntent(true, Some(DrmSystem.Uger)))
    assertValid(s"--backend lsf ${baseCommandLine}", makeExpectedIntent(true, Some(DrmSystem.Lsf)))
    
    assertValid(s"--no-validation ${baseCommandLine}", makeExpectedIntent(false, None))
    assertValid(s"--no-validation --backend uger ${baseCommandLine}", makeExpectedIntent(false, Some(DrmSystem.Uger)))
    assertValid(s"--no-validation --backend lsf ${baseCommandLine}", makeExpectedIntent(false, Some(DrmSystem.Lsf)))
  }
  
  private def assertIsInvalidWithAllDrmSystems(baseCommandLine: String): Unit = {
    assertInvalid(baseCommandLine)
    assertInvalid(s"--backend uger ${baseCommandLine}")
    assertInvalid(s"--backend lsf ${baseCommandLine}")
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
    
    assertValid(
        s"--backend Uger --dry-run --conf $confFile --loams $exampleFile0",
        DryRun(
            confFile = Some(confPath),
            shouldValidate = true,
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.DontFilterByName,
            drmSystemOpt = Option(DrmSystem.Uger),
            loams = paths(exampleFile0)))
    
    assertValid(
        s"--backend Uger --no-validation --dry-run --conf $confFile --loams $exampleFile0",
        DryRun(
            confFile = Some(confPath),
            shouldValidate = false,
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.DontFilterByName,
            drmSystemOpt = Option(DrmSystem.Uger),
            loams = paths(exampleFile0)))
            
    //--compile-only
    assertIsInvalidWithAllDrmSystems("--compile-only") //no loams specified
    assertIsInvalidWithAllDrmSystems("--compile-only --loams") //no loams specified
    
    //--loams  is now required
    assertIsInvalidWithAllDrmSystems(s"--compile-only $exampleFile0 $exampleFile1")
    
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--compile-only --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => CompileOnly(None, shouldValidate, drmSysOpt, paths(exampleFile0, exampleFile1)))
        
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --compile-only --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => 
          CompileOnly(Some(confPath), shouldValidate, drmSysOpt, paths(exampleFile0, exampleFile1)))
        
    assertIsInvalidWithAllDrmSystems(
        s"--conf $confFile --compile-only --loams $exampleFile0 $nonExistentFile") //nonexistent loam file
    
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--compile-only --loams $exampleFile0 $exampleFile1 --conf $confFile",
        (shouldValidate, drmSysOpt) => 
          CompileOnly(Some(confPath), shouldValidate, drmSysOpt, paths(exampleFile0, exampleFile1)))
        
    assertIsValidWithAllDrmSystems(
        s"--compile-only --no-validation --loams $exampleFile0 $exampleFile1 --conf $confFile",
        drmSysOpt => CompileOnly(Some(confPath), false, drmSysOpt, paths(exampleFile0, exampleFile1)))
    
    //--dry-run
    assertInvalid("--dry-run") //no loams specified
    
    //--loams is required
    assertIsInvalidWithAllDrmSystems(s"--dry-run $exampleFile0 $exampleFile1")
    
    assertIsValidWithAllDrmSystems(
        s"--dry-run --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          confFile = None,
          shouldValidate = true,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertIsValidWithAllDrmSystems(
        s"--dry-run --no-validation --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          confFile = None,
          shouldValidate = false,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --dry-run --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          Some(confPath), 
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --dry-run --run everything --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          Some(confPath), 
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.RunEverything,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --dry-run --disable-hashing --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          Some(confPath), 
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.DontHashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --dry-run --run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          Some(confPath), 
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.DontHashOutputs,
          jobFilterIntent = JobFilterIntent.RunEverything,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))

    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--dry-run --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          confFile = None,
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --dry-run --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          Some(confPath), 
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --dry-run --run everything --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          Some(confPath), 
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.RunEverything,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --dry-run --disable-hashing --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          Some(confPath), 
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.DontHashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --dry-run --run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSysOpt) => DryRun(
          Some(confPath), 
          shouldValidate = shouldValidate,
          hashingStrategy = HashingStrategy.DontHashOutputs,
          jobFilterIntent = JobFilterIntent.RunEverything,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
          
    //nonexistent loam file
    assertIsInvalidWithAllDrmSystems(s"--conf $confFile --dry-run --loams $exampleFile0 $nonExistentFile") 
    
    //--run-everything no longer valid, use --run everything instead
    assertIsInvalidWithAllDrmSystems(s"--conf $confFile --run-everything --dry-run --loams $exampleFile0")
    
    //--uger and --lsf gone in favor of --backend {uger,lsf}
    assertIsInvalidWithAllDrmSystems(s"--lsf --conf $confFile --loams $exampleFile0 $exampleFile1")
    assertIsInvalidWithAllDrmSystems(s"--uger --conf $confFile --loams $exampleFile0 $exampleFile1")
    
    //Run some loams
    
    //nonexistent loam file
    assertIsInvalidWithAllDrmSystems(s"--loams $exampleFile0 $nonExistentFile")
    
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSystemOpt) => RealRun(
            confFile = None,
            shouldValidate = shouldValidate,
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.DontFilterByName,
            drmSystemOpt = drmSystemOpt,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSystemOpt) => RealRun(
            confFile = Some(confPath),
            shouldValidate = shouldValidate,
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.DontFilterByName,
            drmSystemOpt = drmSystemOpt,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--run everything --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSystemOpt) => RealRun(
            confFile = None,
            shouldValidate = shouldValidate,
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = drmSystemOpt,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --run everything --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSystemOpt) => RealRun(
            confFile = Some(confPath),
            shouldValidate = shouldValidate,
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = drmSystemOpt,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSystemOpt) => RealRun(
            confFile = None,
            shouldValidate = shouldValidate,
            hashingStrategy = HashingStrategy.DontHashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = drmSystemOpt,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertIsValidWithAllDrmSystemsAndValidationFlags(
        s"--conf $confFile --run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        (shouldValidate, drmSystemOpt) => RealRun(
            confFile = Some(confPath),
            shouldValidate = shouldValidate,
            hashingStrategy = HashingStrategy.DontHashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = drmSystemOpt,
            loams = paths(exampleFile0, exampleFile1)))
            
    val runParams = Seq("foo", "bar", "baz")
    val runRegexes = runParams.map(_.r)  

    def toJobFilterIntent(tag: String): JobFilterIntent = tag match {
      case Conf.RunStrategies.AllOf => JobFilterIntent.RunIfAllMatch(runRegexes)
      case Conf.RunStrategies.AnyOf => JobFilterIntent.RunIfAnyMatch(runRegexes)
      case Conf.RunStrategies.NoneOf => JobFilterIntent.RunIfNoneMatch(runRegexes)
    }
    
    def assertIsValidRealRun(
        commandLine: String, 
        shouldValidate: Boolean,
        drmSystemOpt: Option[DrmSystem], 
        byNameFilterType: String, 
        hashingStrategy: HashingStrategy): Unit = {
      
      val result = cliConf(commandLine).flatMap(from)
    
      assert(result.isRight, s"$result")
        
      val wrapped = result.right.get
    
      val expected = RealRun(
        confFile = Some(confPath),
        shouldValidate = shouldValidate,
        hashingStrategy = hashingStrategy,
        jobFilterIntent = toJobFilterIntent(byNameFilterType),
        drmSystemOpt = drmSystemOpt,
        loams = paths(exampleFile0, exampleFile1))
      
      if(wrapped.isInstanceOf[RealRun] && expected.isInstanceOf[RealRun]) {
        val actualRR = wrapped.asInstanceOf[RealRun]
        val expectedRR = expected.asInstanceOf[RealRun]
        
        def regexStringsFrom(jobFilterIntent: JobFilterIntent): Seq[String] = {
          val regexes = jobFilterIntent match {
            case JobFilterIntent.RunIfAllMatch(res) => res
            case JobFilterIntent.RunIfAnyMatch(res) => res
            case JobFilterIntent.RunIfNoneMatch(res) => res
            case x => Nil
          }
          
          regexes.map(_.pattern.toString)
        }
        
        assert(actualRR.confFile === expectedRR.confFile)
        assert(actualRR.drmSystemOpt === expectedRR.drmSystemOpt)
        assert(actualRR.hashingStrategy === expectedRR.hashingStrategy)
        assert(regexStringsFrom(actualRR.jobFilterIntent) === regexStringsFrom(expectedRR.jobFilterIntent))
        assert(actualRR.loams === expectedRR.loams)
      } else {
        assert(result.right.get === expected)
      }
    }
            
    def doTest(
        shouldValidate: Boolean, 
        drmSystemOpt: Option[DrmSystem], 
        byNameFilterType: String, 
        hashingStrategy: HashingStrategy): Unit = {
      
      val backendPart = drmSystemOpt.map(drmSystem => s"--backend $drmSystem").getOrElse("")
      
      val runPart = s"--run $byNameFilterType ${runParams.mkString(" ")}"
      
      val hashingPart = if(hashingStrategy == HashingStrategy.DontHashOutputs) "--disable-hashing" else ""
      
      val shouldValidatePart = if(shouldValidate) "" else "--no-validation"
        
      val confPart = s"--conf $confFile"
      
      val loamsPart = s"--loams $exampleFile0 $exampleFile1"
        
      val commandLine = s"$shouldValidatePart $backendPart $confPart $runPart $hashingPart $loamsPart".trim
      
      //Now call our own assertValid() so we can have a custom field-by-field equality test, necessary for the regexes
      //in some JobFilterIntents
      
      assertIsValidRealRun(commandLine, shouldValidate, drmSystemOpt, byNameFilterType, hashingStrategy)
    }
    
    for {
      shouldValidate: Boolean <- Seq(true, false)
      drmSystemOpt: Option[DrmSystem] <- Seq(None, Some(DrmSystem.Lsf), Some(DrmSystem.Uger))
      byNameFilterString <- Seq(Conf.RunStrategies.AllOf, Conf.RunStrategies.AnyOf, Conf.RunStrategies.NoneOf)
      hashingStrategy: HashingStrategy <- Seq(HashingStrategy.HashOutputs, HashingStrategy.DontHashOutputs)
    } {
      doTest(shouldValidate, drmSystemOpt, byNameFilterString, hashingStrategy)
    }
  }

  test("determineHashingStrategy") {
    {
      val conf = cliConf(s"--disable-hashing --compile-only --loams $exampleFile0")

      assert(Intent.determineHashingStrategy(conf.right.get.toValues) === HashingStrategy.DontHashOutputs)
    }

    {
      val conf = cliConf(s"--compile-only --loams $exampleFile0")

      assert(Intent.determineHashingStrategy(conf.right.get.toValues) === HashingStrategy.HashOutputs)
    }
  }
}
