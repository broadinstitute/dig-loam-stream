package loamstream.loam.intake

import java.net.URI
import loamstream.loam.LoamScriptContext
import loamstream.loam.NativeTool
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.util.Files
import loamstream.util.Loggable


/**
 * @author clint
 * Feb 19, 2020
 */
abstract class AggregatorIntakeTest extends MysqlTest with Loggable {
  def produceAggregatorEnvFile(dest: Store, s3Bucket: String)(implicit scriptContext: LoamScriptContext): Tool = {
    val result = NativeTool {
      require(dest.isPathStore)
      
      def mungeJdbcUrl(u: String): String = {
        val jdbcColon = "jdbc:"
        
        if(u.startsWith(jdbcColon)) u.drop(jdbcColon.size) else u
      }
      
      val jdbcUrl = new URI(mungeJdbcUrl(container.jdbcUrl))
      
      require(jdbcUrl.getHost != null)
      require(jdbcUrl.getPort > 0)
      
      val envFileData = s"""|INTAKE_S3_BUCKET=${s3Bucket}
                            |INTAKE_DB_HOST=${jdbcUrl.getHost}
                            |INTAKE_DB_PORT=${jdbcUrl.getPort}
                            |INTAKE_DB_USER=${container.username}
                            |INTAKE_DB_PASSWORD=${container.password}
                            |INTAKE_DB_NAME=${container.databaseName}""".stripMargin
                            
      info(s"Writing MySql params:\n${envFileData}")
                            
      Files.writeTo(dest.path)(envFileData)                      
    }
    
    result.out(dest)
  }
}
