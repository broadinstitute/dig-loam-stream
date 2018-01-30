package loamstream

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import loamstream.db.slick.DbDescriptor
import loamstream.db.slick.DbType

/**
 * @author clint
 * Jul 27, 2017
 */
trait IntegrationTestHelpers {
  def getWorkDir: Path = {
    val result = Files.createTempDirectory(getClass.getSimpleName)

    //NB: This seems very heavy-handed, but java.io.File.deleteOnExit doesn't work for non-empty directories. :\
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = FileUtils.deleteQuietly(result.toFile)
    })
    
    result
  }
}

object IntegrationTestHelpers {
  def path(s: String): Path = Paths.get(s)
  
  def withLoudStackTraces[A](f: => A): A = {
    try { f } catch {
      //NB: SBT drastically truncates stack traces. so print them manually to get more info.  
      //This workaround is lame, but gives us a chance at debugging failures.
      case e: Throwable => e.printStackTrace() ; throw e
    }
  }
  
  def inMemoryH2(discriminator: String): DbDescriptor = {
    def makeUrl(dbName: String): String = s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1"
    
    DbDescriptor(DbType.H2, makeUrl(s"integrationtest-${discriminator}"))
  }
}
