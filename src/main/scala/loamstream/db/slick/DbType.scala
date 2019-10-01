package loamstream.db.slick

import slick.jdbc.JdbcProfile

/**
 * @author clint
 * date: Aug 18, 2016
 */
sealed trait DbType {
  def driver: JdbcProfile
  
  def jdbcDriverClass: String
}

object DbType {
  object Hsqldb extends DbType {
    override val driver: JdbcProfile = slick.jdbc.HsqldbProfile
  
    override val jdbcDriverClass: String = "org.hsqldb.jdbc.JDBCDriver"
  }
}
