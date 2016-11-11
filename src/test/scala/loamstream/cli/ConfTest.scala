package loamstream.cli

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite
import org.scalatest.Matchers

/**
 * Created by kyuksel on 10/12/16.
 */
final class ConfTest extends FunSuite with Matchers {
  private val testConfigFile = "src/test/resources/loamstream-test.conf".replace("/", File.separator)

  private def makeConf(args: Seq[String], exitTheJvmOnValidationError: Boolean = false): Conf = {
    Conf(args, exitTheJvmOnValidationError)
  }
  
  test("Single loam file along with conf file is parsed correctly") {
    val conf = makeConf(Seq("--conf", "src/test/resources/loamstream-test.conf", 
                            "--backend", "local", 
                            "src/test/resources/a.txt"))

    conf.loams() shouldEqual List(Paths.get("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }

  test("Multiple loam files along with conf file are parsed correctly") {
    val conf = makeConf(Seq("--conf", "src/test/resources/loamstream-test.conf",
                            "--backend", "local",
                            "src/test/resources/a.txt", "src/test/resources/a.txt"))

    conf.loams() shouldEqual List(Paths.get("src/test/resources/a.txt"), Paths.get("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }
  
  test("Test that we try to exit if passed no args") {
    intercept[CliException] {
      Conf(Seq.empty, exitTheJvmOnValidationError = false)
    }
  }
  
  test("Test that we try to exit if we're passed just --version") {
    try {
      Conf(Seq("--version"), exitTheJvmOnValidationError = false)
      fail()
    } catch {
      case CliException(message) => assert(message === "version") 
    }
  }
   
  test("Test that we try to exit if passed nonexistent file names") {
    intercept[CliException] {
      val args = Seq("--conf", "src/test/resources/loamstream-test.conf",
                     "--backend", "local",
                     "asdfasdf.txt", "src/test/resources/a.txt")
      
      Conf(args, exitTheJvmOnValidationError = false)
    }
    
    intercept[CliException] {
      val args = Seq("--conf", "asdfasdf.txt", "--backend", "local", "src/test/resources/a.txt")
      
      Conf(args, exitTheJvmOnValidationError = false)
    }
  }
  
  test("--compile-only") {
    def doTest(flag: String, loams: Seq[String]): Unit = {
      val args = flag +: loams
      
      val conf = makeConf(args)
      
      assert(conf.compileOnly.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === loams.map(Paths.get(_)))
    }
    
    doTest("--compile-only", Seq("src/main/loam/examples/cp.loam", "src/main/loam/examples/cp.loam"))
    doTest("-c", Seq("src/main/loam/examples/cp.loam", "src/main/loam/examples/cp.loam"))
  }
  
  test("Either --backend or --compile-only must be specified with loam files") {
    //Just a loam file
    intercept[CliException] {
      makeConf(Seq("src/main/loam/cp.loam"))
    }
    
    //loam file, neither --backend nor --compile-only
    intercept[CliException] {
      makeConf(Seq("--conf", "src/main/loam/cp.loam src/main/loam/cp.loam"))
    }
    
    //No loams
    intercept[CliException] {
      makeConf(Seq("--backend", "uger"))
    }
    
    //No loams
    intercept[CliException] {
      makeConf(Seq("--compile-only"))
    }
    
    val exampleFile = "src/main/loam/examples/cp.loam"
    
    val expected = Paths.get(exampleFile)
    
    {
      val conf = makeConf(Seq("--backend", "local", exampleFile, exampleFile))
      
      assert(conf.loams() === Seq(expected, expected))
    }
    
    {
      val conf = makeConf(Seq("--compile-only", exampleFile, exampleFile))
      
      assert(conf.loams() === Seq(expected, expected))
    }
  }
  
  test("--backend") {
    
    val exampleFile = "src/main/loam/examples/cp.loam"
    
    //bogus backend type
    intercept[CliException] {
      makeConf(Seq("--backend", "sldkajalskjd", exampleFile))
    }
    
    //"missing" backend type
    intercept[CliException] {
      makeConf(Seq("--backend", exampleFile))
    }
    
    def doTest(flagValue: String, expected: BackendType): Unit = {
      val conf = makeConf(Seq("--backend", flagValue, exampleFile))
      
      assert(conf.backend() === expected)
    }
    
    doTest("Local", BackendType.Local)
    doTest("local", BackendType.Local)
    doTest("LOCAL", BackendType.Local)
    doTest("LoCaL", BackendType.Local)
    
    doTest("Uger", BackendType.Uger)
    doTest("uger", BackendType.Uger)
    doTest("UGER", BackendType.Uger)
    doTest("UgEr", BackendType.Uger)
  }
}
