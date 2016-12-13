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
 */
final case class OutputRow( loc: String,
                            lastModified: Option[Timestamp],
                            hash: Option[String],
                            executionId: Option[Int] = None) {

  def withExecutionId(newExecutionId: Int): OutputRow = copy(executionId = Some(newExecutionId))

  def toOutputRecord: OutputRecord = OutputRecord(loc, hash, lastModified.map(_.toInstant))

  def toPath: Path = Paths.get(loc)

  def toPathOutput: PathOutput = PathOutput(toPath)

  def toOutput: Output.PathBased = toPathOutput
}

object OutputRow {
  def apply(loc: String): OutputRow = OutputRow(loc, None, None, None)

  def apply(loc: String, hash: String): OutputRow = OutputRow(loc, None, Option(hash), None)

  def apply(loc: String, hash: Option[String]): OutputRow = OutputRow(loc, None, hash, None)

  def apply(rec: OutputRecord): OutputRow = OutputRow(rec.loc, rec.lastModified.map(Timestamp.from), rec.hash, None)
}
