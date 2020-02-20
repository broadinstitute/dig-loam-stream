package loamstream.loam.intake

import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.MySQLContainer
import com.dimafeng.testcontainers.Container
import org.scalatest.FunSuite
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Statement
import java.sql.Connection
import scala.util.control.Exception.Finally

/**
 * @author clint
 * Feb 19, 2020
 */
abstract class MysqlTest extends FunSuite with ForAllTestContainer {
  override val container: MySQLContainer = MySQLContainer()
  
  protected def makeTablesAndThen[A](f: => A): A = {
    makeTables()
    
    f
  }
  
  protected def newConnection: Connection = {
    DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
  }
  
  protected def withStatement[A](connection: Connection)(f: Statement => A): A = {
    val statement = connection.createStatement()
    
    try { f(statement) }
    finally { statement.close() }
  }
  
  protected def withConnection[A](f: Connection => A): A = {
    val connection = newConnection
    
    try { f(connection) }
    finally { connection.close() }
  }
  
  protected def makeTables(): Unit = {
    Class.forName(container.driverClassName)
    
    withConnection { connection => 
      val ddl = {
        """|CREATE TABLE `runs` (
           | `ID` int(11) NOT NULL AUTO_INCREMENT,
           | `uuid` varchar(36) NOT NULL,
           | `processor` varchar(180) NOT NULL,
           | `input` varchar(36) DEFAULT NULL,
           | `output` varchar(200) NOT NULL,
           | `timestamp` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
           | `repo` varchar(2048) NOT NULL,
           | `branch` varchar(200) NOT NULL,
           | `commit` varchar(100) NOT NULL,
           | PRIMARY KEY (`ID`),
           | UNIQUE KEY `APP_IDX` (`processor`,`input`,`output`),
           | KEY `RUN_IDX` (`uuid`)
           |);""".stripMargin
      }
      
      withStatement(connection) {
        _.executeUpdate(ddl)
      }
    }
  }
}
