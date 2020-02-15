package loamstream.loam.intake

import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.MySQLContainer
import com.dimafeng.testcontainers.Container
import org.scalatest.FunSuite
import java.sql.DriverManager
import java.sql.PreparedStatement

final class MysqlTest extends FunSuite with ForAllTestContainer {
  override lazy val container: MySQLContainer = MySQLContainer()
  
  test("make tables") {
    Class.forName(container.driverClassName)
    
    val connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
  
    def withStatement[A](sql: String)(f: PreparedStatement => A): A = {
      val statement = connection.prepareStatement(sql)
      
      try { f(statement) }
      finally { statement.close() }
    }
    
    val ddl = {
"""CREATE TABLE `runs` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `uuid` varchar(36) NOT NULL,
  `processor` varchar(180) NOT NULL,
  `input` varchar(36) DEFAULT NULL,
  `output` varchar(200) NOT NULL,
  `timestamp` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `repo` varchar(2048) NOT NULL,
  `branch` varchar(200) NOT NULL,
  `commit` varchar(100) NOT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `APP_IDX` (`processor`,`input`,`output`),
  KEY `RUN_IDX` (`uuid`)
);"""
    }
    
    val ddlStatement = connection.createStatement()
    
    ddlStatement.executeUpdate(ddl)
    
    ddlStatement.close()
    
    withStatement("select count(*) from runs;") { selectStatement =>
      val resultSet = selectStatement.executeQuery()
    
      assert(resultSet.next() === true)
      
      assert(resultSet.getInt(1) === 0)
      
      assert(resultSet.next() === false)
    
      resultSet.close()
    }
    
    connection.close()
  }
  
  
}
