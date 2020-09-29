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
      assert(conf.clean.isSupplied === false)
      assert(conf.cleanDb.isSupplied === false)
      assert(conf.cleanLogs.isSupplied === false)
      assert(conf.cleanScripts.isSupplied === false)
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
      assert(conf.clean.isSupplied === false)
      assert(conf.cleanDb.isSupplied === false)
      assert(conf.cleanLogs.isSupplied === false)
      assert(conf.cleanScripts.isSupplied === false)
    }
    
    {
      val args = Seq("--compile-only", "--loams", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === Seq(path("src/examples/loam/cp.loam")))
      assert(conf.clean.isSupplied === false)
      assert(conf.cleanDb.isSupplied === false)
      assert(conf.cleanLogs.isSupplied === false)
      assert(conf.cleanScripts.isSupplied === false)
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
      assert(values.cleanDbSupplied === false)
      assert(values.cleanLogsSupplied === false)
      assert(values.cleanScriptsSupplied === false)
    }
    
    {
      val values = makeConf(Seq("-r", "everything", "--loams", exampleFile)).toValues
        
      assert(values.run === Some("everything", Nil))
      assert(values.compileOnlySupplied === false)
      assert(values.loams === Seq(expected))
      assert(values.conf.isEmpty)
      assert(values.cleanDbSupplied === false)
      assert(values.cleanLogsSupplied === false)
      assert(values.cleanScriptsSupplied === false)
    }
    
    {
      val values = makeConf(Seq("--loams", exampleFile)).toValues
        
      assert(values.run === None)
      assert(values.compileOnlySupplied === false)
      assert(values.loams === Seq(expected))
      assert(values.conf.isEmpty)
      assert(values.cleanDbSupplied === false)
      assert(values.cleanLogsSupplied === false)
      assert(values.cleanScriptsSupplied === false)
    }
  }
  
  test("Values.toArguments - simple cases") {
    def doSimpleTest(flag: String): Unit = {
      val args = Seq(flag)
      
      val conf = makeConf(args)
    
      assert(conf.toValues.toArguments === args)
    }
    
    doSimpleTest("--clean")
    doSimpleTest("--dry-run")
    doSimpleTest("--disable-hashing")
    doSimpleTest("--worker")
    doSimpleTest("--clean-db")
    doSimpleTest("--clean-logs")
    doSimpleTest("--clean-scripts")
    doSimpleTest("--compile-only")
    doSimpleTest("--no-validation")
  }
  
  test("Values.toArguments - real-world") {
    def doTest(argLine: String): Unit = {
      val args: Seq[String] = argLine.split("\\s+").toList
    
      val conf = Conf(args)
    
      assert(conf.toValues.toArguments === args)
    }
    
    doTest("--conf foo.conf --backend uger --run allOf bar --loams x.loam y.loam")
    doTest("--conf foo.conf --backend lsf --run anyOf baz --loams a.loam b.loam c.loam")
    doTest("--conf foo.conf --backend uger --run noneOf foo --loams a.loam b.loam c.loam")
    doTest("--conf foo.conf --backend uger --run everything --loams u.loam v.loam")
  }
  
  test("Values.toArguments - --worker is added") {
    val argLine = "--conf foo.conf --backend uger --loams u.loam v.loam"
    
    val args: Seq[String] = argLine.split("\\s+").toList
    
    val values = Conf(args).toValues
    
    assert(values.workerSupplied === false)
    
    val withWorker = values.withIsWorker(true)
    
    assert(withWorker.workerSupplied === true)
    
    assert(values.toArguments === args)
    
    assert(withWorker.toArguments === ("--worker" +: args))
  }
}
