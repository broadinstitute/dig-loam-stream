package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat


/**
 * @author clint
 * Dec 17, 2019
 */
trait Renderer {
  def render(row: Row): String
}

final case class CommonsCsvRenderer(csvFormat: CSVFormat) extends Renderer {
  override def render(row: Row): String = csvFormat.format(row.values: _*)
}
