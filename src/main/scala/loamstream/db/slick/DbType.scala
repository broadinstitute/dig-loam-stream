package loamstream.db.slick

import slick.driver.JdbcProfile

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
    override val driver: JdbcProfile = slick.driver.H2Driver
  
    override val jdbcDriverClass: String = "org.h2.Driver"
  }
}