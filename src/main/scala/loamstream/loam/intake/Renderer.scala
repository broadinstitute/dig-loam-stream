package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat


/**
 * @author clint
 * Dec 17, 2019
 */
final case class Renderer(csvFormat: CSVFormat) {
  def render(row: Row): String = csvFormat.format(row.values: _*)
}
