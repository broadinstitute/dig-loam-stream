package loamstream.util.jvm

import java.lang.management.ManagementFactory
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.cli.Conf
import scala.collection.compat._

import scala.collection.compat._

/**
 * @author clint
 * Aug 10, 2020
 */
final case class JvmArgs(jvmArgs: Seq[String], classpath: String) {
  def javaBinary: Path = {
    import loamstream.util.Paths.Implicits.PathHelpers
    
    val javaBinaryPath = Option(System.getProperty("java.home")).map(Paths.get(_)) match {
      case Some(javaHome) => javaHome / "bin" / "java"
      case None => sys.error("JAVA_HOME must be defined")
    }
    
    javaBinaryPath.toAbsolutePath
  }
  
  def rerunCommandTokens(conf: Conf.Values, systemProperties: Map[String, String]): Seq[String] = {
    val sysprops = systemProperties.to(Seq).sortBy { case (k, _) => k }.map { case (k, v) => s"-D${k}=${v}" }
    
    Seq(javaBinary.toString) ++
    jvmArgs ++ 
    sysprops ++
    Seq("-jar", classpath) ++
    conf.toArguments
  }
}

object JvmArgs {
  def apply(): JvmArgs = JvmArgs(jvmArgsForThisRun, classpathForThisRun)
  
  import scala.collection.JavaConverters._
  
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
  private[jvm] def jvmArgsForThisRun: Seq[String] = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.to(Seq)
  }
  
  private[jvm] def classpathForThisRun: String = ManagementFactory.getRuntimeMXBean.getClassPath
}
