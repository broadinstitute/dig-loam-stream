package loamstream

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import loamstream.db.slick.DbDescriptor
import loamstream.db.slick.DbType
import loamstream.util.Sequence

/**
 * @author clint
 * Jul 27, 2017
 */
trait IntegrationTestHelpers {
  def getWorkDir: Path = {
    val result = Files.createTempDirectory(getClass.getSimpleName)

    deleteAtExit(result)
  }
  
  def getWorkDirUnderTarget(subDir: Option[String] = None): Path = {
    import IntegrationTestHelpers.sequence
    import java.nio.file.Files.exists
    
    val subDirParts = Seq(s"${getClass.getSimpleName}-${sequence.next()}") ++ subDir
    
    val result = Paths.get("target", subDirParts: _*).toAbsolutePath
    
    result.toFile.mkdirs()
    
    require(exists(result))

    deleteAtExit(result)
  }
  
  private def deleteAtExit(p: Path): Path = {
    //NB: This seems very heavy-handed, but java.io.File.deleteOnExit doesn't work for non-empty directories. :\
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = FileUtils.deleteQuietly(p.toFile)
    })
    
    p
  }
}

object IntegrationTestHelpers {
  private val sequence: Sequence[Int] = Sequence()
  
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
