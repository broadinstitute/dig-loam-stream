package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat


/**
 * @author clint
 * Dec 17, 2019
 */
trait Renderer {
  def render(row: RenderableRow): String
}

object Renderer {
  final case class CommonsCsv(csvFormat: CSVFormat) extends Renderer {
    override def render(row: RenderableRow): String = {
      // TODO: Look into this, flatten is fine, but the headers could be off as it eliminates optionals
      val unpacked = row.values.flatten//map(_.getOrElse(null)) //TODO: null ok?
      
      csvFormat.format(unpacked: _*)
    }
  }  
}


