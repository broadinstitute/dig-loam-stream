package loamstream.db.slick

import slick.driver.JdbcProfile

/**
 * @author clint
 * date: Aug 10, 2016
 */
final case class DbDescriptor(driver: JdbcProfile, url: String, jdbcDriverClass: String)