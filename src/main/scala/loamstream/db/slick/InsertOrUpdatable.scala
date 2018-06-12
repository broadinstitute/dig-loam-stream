package loamstream.db.slick

/**
 * @author clint
 * Jun 11, 2018
 */
trait InsertOrUpdatable {
  //NB: Double-dispatch pattern, to avoid repeated pattern-matches in SlickLoamDao.
  def insertOrUpdate(tables: Tables): tables.driver.api.DBIO[Int]
}
