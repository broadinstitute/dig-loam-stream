package loamstream.loam.intake

import java.net.URI

import loamstream.loam.LoamScriptContext
import loamstream.loam.NativeTool
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.util.Files


/**
 * @author clint
 * Feb 19, 2020
 */
abstract class AggregatorIntakeTest extends MysqlTest {
  def produceAggregatorEnvFile(dest: Store, s3Bucket: String)(implicit scriptContext: LoamScriptContext): Tool = {
    val result = NativeTool {
      require(dest.isPathStore)
      
      val jdbcUrl = new URI(container.jdbcUrl)
      
      val hostAndPort = s"${jdbcUrl.getHost}:${jdbcUrl.getPort}"
      
      val envFileData = s"""|INTAKE_S3_BUCKET=${s3Bucket}
                            |INTAKE_DB_HOST=${hostAndPort}
                            |INTAKE_DB_USER=${container.username}
                            |INTAKE_DB_PASSWORD=${container.password}
                            |INTAKE_DB_NAME=${container.databaseName}""".stripMargin
                            
      Files.writeTo(dest.path)(envFileData)                      
    }
    
    result.out(dest)
  }
}
