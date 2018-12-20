package loamstream.cli

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite
import org.scalatest.Matchers
import java.net.URI
import loamstream.TestHelpers

/**
 * Created by kyuksel on 10/12/16.
 */
final class ConfTest extends FunSuite with Matchers {
  private val testConfigFile = "src/test/resources/loamstream-test.conf".replace("/", File.separator)

  private def makeConf(args: Seq[String]): Conf = Conf(args)
  
  import TestHelpers.path
  
  test("--clean") {
    {
      val conf = makeConf(Seq("--clean"))
      
      assert(conf.clean.isSupplied === true)
      assert(conf.cleanDb.isSupplied === false)
      assert(conf.cleanLogs.isSupplied === false)
      assert(conf.cleanScripts.isSupplied === false)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.backend.isSupplied === false)
      assert(conf.loams.isSupplied === false)
    }
    
    {
      val conf = makeConf("--clean-db --clean-logs --clean-scripts".split("\\s+"))
      
      assert(conf.clean.isSupplied === false)
      assert(conf.cleanDb.isSupplied === true)
      assert(conf.cleanLogs.isSupplied === true)
      assert(conf.cleanScripts.isSupplied === true)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.backend.isSupplied === false)
      assert(conf.loams.isSupplied === false)
    }
    
    {
      val conf = makeConf("--clean --clean-db --clean-logs --clean-scripts".split("\\s+"))
      
      assert(conf.clean.isSupplied === true)
      assert(conf.cleanDb.isSupplied === true)
      assert(conf.cleanLogs.isSupplied === true)
      assert(conf.cleanScripts.isSupplied === true)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.backend.isSupplied === false)
      assert(conf.loams.isSupplied === false)
    }
    
  }
  
  test("--backend") {
    {
      val args = Seq("--loams", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.backend.isSupplied === false)
      assert(conf.loams() === Seq(path("src/examples/loam/cp.loam")))
      assert(conf.clean.isSupplied === false)
      assert(conf.cleanDb.isSupplied === false)
      assert(conf.cleanLogs.isSupplied === false)
      assert(conf.cleanScripts.isSupplied === false)
    }
    
    {
      val args = Seq("--backend", "lsf", "--loams", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.backend() == "lsf")
      assert(conf.loams() === Seq(path("src/examples/loam/cp.loam")))
      assert(conf.clean.isSupplied === false)
      assert(conf.cleanDb.isSupplied === false)
      assert(conf.cleanLogs.isSupplied === false)
      assert(conf.cleanScripts.isSupplied === false)
    }
    
    {
      val args = Seq("--backend", "uger", "--loams", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.backend() === "uger")
      assert(conf.loams() === Seq(path("src/examples/loam/cp.loam")))
      assert(conf.clean.isSupplied === false)
      assert(conf.cleanDb.isSupplied === false)
      assert(conf.cleanLogs.isSupplied === false)
      assert(conf.cleanScripts.isSupplied === false)
    }
  }
  
  test("Single loam file along with conf file is parsed correctly") {
    val conf = makeConf(Seq("--conf", "src/test/resources/loamstream-test.conf", "--loams", 
                            "src/test/resources/a.txt"))

    conf.loams() shouldEqual List(path("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }

  test("Multiple loam files along with conf file are parsed correctly") {
    val conf = makeConf(Seq("--conf", "src/test/resources/loamstream-test.conf", "--loams", 
                            "src/test/resources/a.txt", "src/test/resources/a.txt"))

    conf.loams() shouldEqual List(path("src/test/resources/a.txt"), path("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }
  
  test("--compile-only") {
    def doTest(flag: String, loams: Seq[String]): Unit = {
      val args = flag +: "--loams" +: loams
      
      val conf = makeConf(args)
      
      assert(conf.compileOnly.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === loams.map(path(_)))
    }
    
    doTest("--compile-only", Seq("src/examples/loam/cp.loam", "src/examples/loam/cp.loam"))
  }
  
  test("--disable-hashing") {
    {
      val args = Seq("--disable-hashing", "--compile-only", "--loams", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied)
      assert(conf.compileOnly.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === Seq(path("src/examples/loam/cp.loam")))
    }
    
    {
      val args = Seq("--compile-only", "--loams", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === Seq(path("src/examples/loam/cp.loam")))
    }
  }
  
  test("--run everything") {
    val exampleFile = "src/examples/loam/cp.loam"
    
    val expected = path(exampleFile)
    
    {
      val values = makeConf(Seq("--run", "everything", "--loams", exampleFile)).toValues
      
      assert(values.run === Some("everything", Nil))
      assert(values.compileOnlySupplied === false)
      assert(values.loams === Seq(expected))
      assert(values.conf.isEmpty)
    }
    
    {
      val values = makeConf(Seq("-r", "everything", "--loams", exampleFile)).toValues
        
      assert(values.run === Some("everything", Nil))
      assert(values.compileOnlySupplied === false)
      assert(values.loams === Seq(expected))
      assert(values.conf.isEmpty)
    }
    
    {
      val values = makeConf(Seq("--loams", exampleFile)).toValues
        
      assert(values.run === None)
      assert(values.compileOnlySupplied === false)
      assert(values.loams === Seq(expected))
      assert(values.conf.isEmpty)
    }
  }
  
  test("--lookup") {
    val someFile = "some/arbitrary/output"
    val someUri = "gs://foo/bar/baz"
    
    val expectedPath = path(someFile)
    val expectedUri = URI.create(someUri)
    
    //Path, full arg name
    {
      val values = makeConf(Seq("--lookup", someFile)).toValues
        
      assert(values.lookup === Some(Left(expectedPath)))
      
      assert(values.run === None)
      assert(values.loams === Nil)
      assert(values.compileOnlySupplied === false)
      assert(values.conf === None)
    }
    //URI, full arg name
    {
      val values = makeConf(Seq("--lookup", someUri)).toValues
        
      assert(values.lookup === Some(Right(expectedUri)))
      
      assert(values.run === None)
      assert(values.loams === Nil)
      assert(values.compileOnlySupplied === false)
      assert(values.conf === None)
    }
  }
}
