package loamstream.util.jvm

import java.lang.management.ManagementFactory

import scala.collection.JavaConverters._
import java.nio.file.Paths
import java.nio.file.Path
import loamstream.cli.Conf

/**
 * @author clint
 * Aug 10, 2020
 */
object JvmArgs extends App {
  /**
   * The JVM-level args this program was run with:
   * 
   * -Xmx2g
   * -Xss2m
   * -Dfoo=bar
   * 
   * etc.  
   * 
   * BUT NOT the application-level args: 
   * --loams Foo.scala 
   * --backend uger
   * 
   * etc.
   */
  def jvmArgsForThisRun: Iterable[String] = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
  }
  
  def classpathForThisRun: String = {
    ManagementFactory.getRuntimeMXBean.getClassPath
  }
  
  def javaBinary: Path = {
    import loamstream.util.Paths.Implicits.PathHelpers
    
    val javaBinaryPath = Option(System.getProperty("java.home")).map(Paths.get(_)) match {
      case Some(javaHome) => javaHome / "bin" / "java"
      case None => sys.error("JAVA_HOME must be defined")
    }
    
    javaBinaryPath.toAbsolutePath
  }
  
  def rerunCommandTokens(conf: Conf): Seq[String] = {
    Seq(javaBinary.toString) ++
    Seq("-jar", classpathForThisRun) ++
    jvmArgsForThisRun ++ 
    conf.arguments
  }
  
  val conf = Conf("--backend lsf --loams foo.scala".split("\\s+"))
  
  rerunCommandTokens(conf).foreach(println)
}
