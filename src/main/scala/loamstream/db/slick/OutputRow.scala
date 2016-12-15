package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Timestamp

import loamstream.model.jobs.{Output, OutputRecord}
import loamstream.model.jobs.Output.PathOutput

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
                            executionId: Option[Int] = None) {

  def this(loc: String, hash: Option[String]) = {
    this(
      loc,
      None,
      hash,
      None)
  }

  def this(loc: String) = {
    this(
      loc,
      None,
      None,
      None)
  }

  def this(rec: OutputRecord) = {
    this(
      rec.loc,
      rec.lastModified.map(Timestamp.from),
      rec.hash,
      None)
  }

  def withExecutionId(newExecutionId: Int): OutputRow = copy(executionId = Some(newExecutionId))

  def toOutputRecord: OutputRecord = OutputRecord(loc, hash, lastModified.map(_.toInstant))

  def toPath: Path = Paths.get(loc)

  def toPathOutput: PathOutput = PathOutput(toPath)

  def toOutput: Output.PathBased = toPathOutput
}
