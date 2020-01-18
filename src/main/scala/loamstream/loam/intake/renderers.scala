package loamstream.loam.intake

import org.apache.commons.csv.CSVFormat
import de.siegmar.fastcsv.writer.CsvWriter
import java.io.StringWriter


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

final case class FastCsvRenderer(delimiter: Char = CsvSource.Defaults.FastCsv.delimiter) extends Renderer {
  override def render(row: Row): String = {
    val csvWriter = new CsvWriter

    csvWriter.setFieldSeparator(delimiter)
    
    val stringWriter = new StringWriter
    
    val csvAppender = csvWriter.append(stringWriter)
    
    try { 
      csvAppender.appendLine(row.values: _*)
      
      stringWriter.toString
    } finally {
      stringWriter.close()
      csvAppender.close()
    }
  }
}
