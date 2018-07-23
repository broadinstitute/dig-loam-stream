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
    
    //--compile-only
    assertIsInvalidWithAllDrmSystems("--compile-only") //no loams specified
    assertIsInvalidWithAllDrmSystems("--compile-only --loams") //no loams specified
    
    //--loams  is now required
    assertIsInvalidWithAllDrmSystems(s"--compile-only $exampleFile0 $exampleFile1")
    
    assertIsValidWithAllDrmSystems(
        s"--compile-only --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => CompileOnly(None, drmSysOpt, paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --compile-only --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => CompileOnly(Some(confPath), drmSysOpt, paths(exampleFile0, exampleFile1)))
    
    assertIsInvalidWithAllDrmSystems(
        s"--conf $confFile --compile-only --loams $exampleFile0 $nonExistentFile") //nonexistent loam file
    
    assertIsValidWithAllDrmSystems(
        s"--compile-only --loams $exampleFile0 $exampleFile1 --conf $confFile",
        drmSysOpt => CompileOnly(Some(confPath), drmSysOpt, paths(exampleFile0, exampleFile1)))
    
    //--dry-run
    assertInvalid("--dry-run") //no loams specified
    
    //--loams is required
    assertIsInvalidWithAllDrmSystems(s"--dry-run $exampleFile0 $exampleFile1")
    
    assertIsValidWithAllDrmSystems(
        s"--dry-run --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          confFile = None,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --dry-run --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --dry-run --run everything --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.RunEverything,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --dry-run --disable-hashing --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.DontHashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --dry-run --run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.DontHashOutputs,
          jobFilterIntent = JobFilterIntent.RunEverything,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))

    assertIsValidWithAllDrmSystems(
        s"--dry-run --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          confFile = None,
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --dry-run --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
          
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --dry-run --run everything --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.HashOutputs,
          jobFilterIntent = JobFilterIntent.RunEverything,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --dry-run --disable-hashing --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          Some(confPath), 
          hashingStrategy = HashingStrategy.DontHashOutputs,
          jobFilterIntent = JobFilterIntent.DontFilterByName,
          drmSystemOpt = drmSysOpt,
          loams = paths(exampleFile0, exampleFile1)))
    
    assertIsValidWithAllDrmSystems(
        s"--conf $confFile --dry-run --run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        drmSysOpt => DryRun(
          Some(confPath), 
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
    
    //--lookup
    assertIsInvalidWithAllDrmSystems("--lookup") //no output file
    assertIsInvalidWithAllDrmSystems(s"--conf $confFile --lookup") //no output file
    
    assertValid(s"--lookup $outputFile", LookupOutput(None, Left(path(outputFile))))
    assertValid(s"--lookup $outputUri", LookupOutput(None, Right(uri(outputUri))))
    
    assertValid(s"--conf $confFile --lookup $outputFile", LookupOutput(Some(confPath), Left(path(outputFile))))
    assertValid(s"--conf $confFile --lookup $outputUri", LookupOutput(Some(confPath), Right(uri(outputUri))))
    
    //Run some loams
    
    //nonexistent loam file
    assertIsInvalidWithAllDrmSystems(s"--loams $exampleFile0 $nonExistentFile")
    
    assertValid(
        s"--loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = None,
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.DontFilterByName,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.DontFilterByName,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--backend uger --conf $confFile --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.DontFilterByName,
            drmSystemOpt = Some(DrmSystem.Uger),
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--backend lsf --conf $confFile --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.DontFilterByName,
            drmSystemOpt = Some(DrmSystem.Lsf),
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--run everything --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = None,
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile --run everything --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.HashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = None,
            hashingStrategy = HashingStrategy.DontHashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--conf $confFile --run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.DontHashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = None,
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--backend lsf --conf $confFile --run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.DontHashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = Some(DrmSystem.Lsf),
            loams = paths(exampleFile0, exampleFile1)))
            
    assertValid(
        s"--backend uger --conf $confFile --run everything --disable-hashing --loams $exampleFile0 $exampleFile1", 
        RealRun(
            confFile = Some(confPath),
            hashingStrategy = HashingStrategy.DontHashOutputs,
            jobFilterIntent = JobFilterIntent.RunEverything,
            drmSystemOpt = Some(DrmSystem.Uger),
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
        drmSystemOpt: Option[DrmSystem], 
        byNameFilterType: String, 
        hashingStrategy: HashingStrategy): Unit = {
      
      val result = cliConf(commandLine).flatMap(from)
    
      assert(result.isRight, s"$result")
        
      val wrapped = result.right.get
    
      val expected = RealRun(
        confFile = Some(confPath),
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
            
    def doTest(drmSystemOpt: Option[DrmSystem], byNameFilterType: String, hashingStrategy: HashingStrategy): Unit = {
      val backendPart = drmSystemOpt.map(drmSystem => s"--backend $drmSystem").getOrElse("")
      
      val runPart = s"--run $byNameFilterType ${runParams.mkString(" ")}"
      
      val hashingPart = if(hashingStrategy == HashingStrategy.DontHashOutputs) "--disable-hashing" else ""
      
      val commandLine = s"$backendPart --conf $confFile $runPart $hashingPart --loams $exampleFile0 $exampleFile1".trim
      
      //Now call our own assertValid() so we can have a custom field-by-field equality test, necessary for the regexes
      //in some JobFilterIntents
      
      assertIsValidRealRun(commandLine, drmSystemOpt, byNameFilterType, hashingStrategy)
    }
    
    for {
      drmSystemOpt: Option[DrmSystem] <- Seq(None, Some(DrmSystem.Lsf), Some(DrmSystem.Uger))
      byNameFilterString <- Seq(Conf.RunStrategies.AllOf, Conf.RunStrategies.AnyOf, Conf.RunStrategies.NoneOf)
      hashingStrategy: HashingStrategy <- Seq(HashingStrategy.HashOutputs, HashingStrategy.DontHashOutputs)
    } {
      doTest(drmSystemOpt, byNameFilterString, hashingStrategy)
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
