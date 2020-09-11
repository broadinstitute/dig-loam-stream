package loamstream.db.slick

import java.sql.Timestamp
import loamstream.model.execute.Run


/**
 * @author clint
 *         date: Sep 22, 2016
 */
final case class RunRow(id: Int, name: String, timeStamp: Timestamp) {
  def toRun: Run = Run(name, timeStamp.toLocalDateTime)
}
