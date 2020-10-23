package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat


/**
 * @author clint
 * Dec 17, 2019
 */
trait Row {
  def values: Seq[String]
}

final case class LiteralRow(values: Seq[String]) extends Row

object LiteralRow {
  def apply(values: String*)(implicit discriminator: Int = 42): LiteralRow = new LiteralRow(values) 
}
