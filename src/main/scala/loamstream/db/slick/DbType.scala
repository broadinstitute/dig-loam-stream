package loamstream.db.slick

import slick.jdbc.JdbcProfile

/**
 * @author clint
 * date: Aug 18, 2016
 */
trait DbType {
  def driver: JdbcProfile
  
  def jdbcDriverClass: String
}

object DbType {
  object H2 extends DbType {
    override val driver: JdbcProfile = slick.jdbc.H2Profile
  
    override val jdbcDriverClass: String = "org.h2.Driver"
  }
  
  object Hsqldb extends DbType {
    override val driver: JdbcProfile = slick.jdbc.HsqldbProfile
  
    override val jdbcDriverClass: String = "org.hsqldb.jdbc.JDBCDriver"
  }
}
