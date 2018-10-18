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
object IntegrationTestHelpers {
  private val sequence: Sequence[Int] = Sequence()
  
  def path(s: String): Path = Paths.get(s)
  
  def inMemoryH2(discriminator: String): DbDescriptor = {
    def makeUrl(dbName: String): String = s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1"
    
    DbDescriptor(DbType.H2, makeUrl(s"integrationtest-${discriminator}"))
  }
  
  def getWorkDirUnderTarget(subDir: Option[String] = None): Path = {
    val classNamePart = getClass.getSimpleName match {
      case cn if cn.endsWith("$") => cn.dropRight(1)
      case cn => cn
    }
    
    val subDirParts = Seq(s"${classNamePart}-${sequence.next()}") ++ subDir
    
    val result = Paths.get("target", subDirParts: _*).toAbsolutePath
    
    try { 
      result 
    } finally {
      result.toFile.mkdirs()
    }
  }
}
