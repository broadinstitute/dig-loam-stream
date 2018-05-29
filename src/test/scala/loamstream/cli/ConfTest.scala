package loamstream.cli

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite
import org.scalatest.Matchers
import java.net.URI

/**
 * Created by kyuksel on 10/12/16.
 */
final class ConfTest extends FunSuite with Matchers {
  private val testConfigFile = "src/test/resources/loamstream-test.conf".replace("/", File.separator)

  private def makeConf(args: Seq[String]): Conf = Conf(args)
  
  test("--uger, --lsf") {
    {
      val args = Seq("src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.uger.isSupplied === false)
      assert(conf.lsf.isSupplied === false)
      assert(conf.loams() === Seq(Paths.get("src/examples/loam/cp.loam")))
    }
    
    {
      val args = Seq("--lsf", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.uger.isSupplied === false)
      assert(conf.lsf.isSupplied === true)
      assert(conf.loams() === Seq(Paths.get("src/examples/loam/cp.loam")))
    }
    
    {
      val args = Seq("--uger", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied === false)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.uger.isSupplied === true)
      assert(conf.lsf.isSupplied === false)
      assert(conf.loams() === Seq(Paths.get("src/examples/loam/cp.loam")))
    }
  }
  
  test("Single loam file along with conf file is parsed correctly") {
    val conf = makeConf(Seq("--conf", "src/test/resources/loamstream-test.conf", 
                            "src/test/resources/a.txt"))

    conf.loams() shouldEqual List(Paths.get("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
  }

  test("Multiple loam files along with conf file are parsed correctly") {
    val conf = makeConf(Seq("--conf", "src/test/resources/loamstream-test.conf",
                            "src/test/resources/a.txt", "src/test/resources/a.txt"))

    conf.loams() shouldEqual List(Paths.get("src/test/resources/a.txt"), Paths.get("src/test/resources/a.txt"))
    conf.conf().toString shouldEqual testConfigFile
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
    
    doTest("--compile-only", Seq("src/examples/loam/cp.loam", "src/examples/loam/cp.loam"))
    doTest("-c", Seq("src/examples/loam/cp.loam", "src/examples/loam/cp.loam"))
  }
  
  test("--disable-hashing") {
    {
      val args = Seq("--disable-hashing", "--compile-only", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied)
      assert(conf.compileOnly.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === Seq(Paths.get("src/examples/loam/cp.loam")))
    }
    
    {
      val args = Seq("--compile-only", "src/examples/loam/cp.loam")
      
      val conf = makeConf(args)
      
      assert(conf.disableHashing.isSupplied === false)
      assert(conf.compileOnly.isSupplied)
      assert(conf.conf.isSupplied === false)
      assert(conf.version.isSupplied === false)
      assert(conf.loams() === Seq(Paths.get("src/examples/loam/cp.loam")))
    }
  }
  
  test("--run-everything") {
    val exampleFile = "src/examples/loam/cp.loam"
    
    val expected = Paths.get(exampleFile)
    
    {
      val conf = makeConf(Seq("--run-everything", exampleFile))
        
      conf.runEverything() shouldBe(true)
      conf.loams() shouldEqual List(expected)
      conf.compileOnly() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    
    {
      val conf = makeConf(Seq("-r", exampleFile))
        
      conf.runEverything() shouldBe(true)
      conf.loams() shouldEqual List(expected)
      conf.compileOnly() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    
    {
      val conf = makeConf(Seq(exampleFile))
        
      conf.runEverything() shouldBe(false)
      conf.loams() shouldEqual List(expected)
      conf.compileOnly() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
  }
  
  test("--lookup") {
    val someFile = "some/arbitrary/output"
    val someUri = "gs://foo/bar/baz"
    
    val expectedPath = Paths.get(someFile)
    val expectedUri = URI.create(someUri)
    
    //Path, full arg name
    {
      val conf = makeConf(Seq("--lookup", someFile))
        
      conf.lookup.isSupplied shouldBe(true)
      conf.lookup() shouldBe(Left(expectedPath))
      
      conf.runEverything() shouldBe(false)
      conf.loams.isSupplied shouldBe(false)
      conf.compileOnly() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    //Path, short arg name
    {
      val conf = makeConf(Seq("-l", someFile))
        
      conf.lookup.isSupplied shouldBe(true)
      conf.lookup() shouldBe(Left(expectedPath))
      
      conf.runEverything() shouldBe(false)
      conf.loams.isSupplied shouldBe(false)
      conf.compileOnly() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    
    //URI, full arg name
    {
      val conf = makeConf(Seq("--lookup", someUri))
        
      conf.lookup.isSupplied shouldBe(true)
      conf.lookup() shouldBe(Right(expectedUri))
      
      conf.runEverything() shouldBe(false)
      conf.loams.isSupplied shouldBe(false)
      conf.compileOnly() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
    //URI, short arg name
    {
      val conf = makeConf(Seq("-l", someUri))
        
      conf.lookup.isSupplied shouldBe(true)
      conf.lookup() shouldBe(Right(expectedUri))
      
      conf.runEverything() shouldBe(false)
      conf.loams.isSupplied shouldBe(false)
      conf.compileOnly() shouldBe(false)
      conf.conf.isSupplied shouldBe(false)
    }
  }
}
