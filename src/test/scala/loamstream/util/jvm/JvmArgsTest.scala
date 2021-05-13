package loamstream.util.jvm

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.cli.Conf
import scala.collection.compat._

/**
 * @author clint
 * Sep 29, 2020
 */
final class JvmArgsTest extends FunSuite {
  test("javaBinary") {
    val jvmArgs = JvmArgs(Seq("jvmArg0", "jvmArg1", "jvmArg2"), "some-class-path")
    
    val expected = Paths.get(System.getProperty("java.home")).resolve("bin").resolve("java")
    
    assert(jvmArgs.javaBinary === expected)
  }
  
  test("rerunCommandTokens") {
    val jvmArgs = JvmArgs(Seq("jvmArg0", "jvmArg1", "jvmArg2"), "some-class-path")
    
    val expectedJavaBinary = Paths.get(System.getProperty("java.home")).resolve("bin").resolve("java")
    
    val syspropMap = Map("x" -> "123", "y" -> "456")
    
    val sysprop0 = "-Dx=123"
    val sysprop1 = "-Dy=456"
    
    val args = "--conf foo.conf --backend uger --run everything --loams u.loam v.loam".split("\\s+").to(List)
    
    val expected = Seq(
        expectedJavaBinary.toString,
        "jvmArg0", 
        "jvmArg1", 
        "jvmArg2",
        sysprop0,
        sysprop1,
        "-jar",
        "some-class-path",
        "--worker") ++ args
        
    assert(jvmArgs.rerunCommandTokens(Conf(args).toValues.withIsWorker(true), syspropMap) === expected)
  }
}
