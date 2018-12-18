package loamstream.db.slick

import java.sql.Timestamp

import loamstream.model.jobs.StoreRecord

/**
 * @author clint
 *         kyuksel
 * date: Aug 4, 2016
 *
 * A class representing a row in the 'Outputs' table.
 *
 * Note:  Constructor overloads are chosen in lieu of companion object 'apply's to appease Slick's <> operator
 *        along with Function.tupled (see Tables.scala for the usage)
 */
final case class OutputRow( loc: String,
                            lastModified: Option[Timestamp],
                            hash: Option[String],
                            hashType: Option[String],
                            executionId: Option[Int] = None) {

  def this(loc: String) = {
    this(
      loc,
      None,
      None,
      None,
      None)
  }

  def this(loc: String, hash: Option[String]) = {
    this(
      loc,
      None,
      None,
      None,
      None)
  }

  def this(loc: String, hash: Option[String], hashType: Option[String]) = {
    this(
      loc,
      None,
      hash,
      hashType,
      None)
  }

  def this(rec: StoreRecord) = {
    this(
      rec.loc,
      rec.lastModified.map(Timestamp.from),
      rec.hash,
      rec.hashType,
      None)
  }

  def withExecutionId(newExecutionId: Int): OutputRow = copy(executionId = Some(newExecutionId))

  def toOutputRecord: StoreRecord = StoreRecord(loc, hash, hashType, lastModified.map(_.toInstant))
}
