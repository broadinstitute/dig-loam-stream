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
  def getWorkDir: Path = Files.createTempDirectory(getClass.getSimpleName)
  
  def getWorkDirUnderTarget(subDir: Option[String] = None): Path = {
    import IntegrationTestHelpers.sequence
    
    val subDirParts = Seq(s"${getClass.getSimpleName}-${sequence.next()}") ++ subDir
    
    val result = Paths.get("target", subDirParts: _*).toAbsolutePath
    
    try { 
      result 
    } finally {
      result.toFile.mkdirs()
    }
  }
}

object IntegrationTestHelpers {
  private val sequence: Sequence[Int] = Sequence()
  
  def path(s: String): Path = Paths.get(s)
  
  def inMemoryH2(discriminator: String): DbDescriptor = {
    def makeUrl(dbName: String): String = s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1"
    
    DbDescriptor(DbType.H2, makeUrl(s"integrationtest-${discriminator}"))
  }
}
