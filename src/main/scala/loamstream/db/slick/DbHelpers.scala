package loamstream.db.slick

import slick.jdbc.JdbcProfile
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.sql.Timestamp
import java.time.Instant

/**
 * @author clint
 * Jan 11, 2018
 */
trait DbHelpers {
  val driver: JdbcProfile
  
  import driver.api._
  
  //TODO: Re-evaluate; block all the time?
  private[slick] def runBlocking[A](db: Database)(action: DBIO[A]): A = blockOn(db.run(action))
  
  private[slick] def blockOn[A](future: Future[A]): A = Await.result(future, Duration.Inf)
}

/**
 * @author clint
 * date: Aug 10, 2016
 */
object DbHelpers {
  def timestampFromLong(millisSinceEpoch: Long): Timestamp = {
    Timestamp.from(Instant.ofEpochMilli(millisSinceEpoch))
  }
  
  val dummyId: Int = -1
}
