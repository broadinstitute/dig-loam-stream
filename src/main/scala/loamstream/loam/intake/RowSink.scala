package loamstream.loam.intake

import java.io.Closeable
import java.nio.file.Path
import org.apache.commons.csv.CSVFormat
import java.io.Writer
import java.nio.file.Files
import java.nio.charset.StandardCharsets
import loamstream.util.ValueBox

/**
 * @author clint
 * Oct 13, 2020
 */
trait RowSink extends Closeable {
  def accept(row: Row): Unit
}

object RowSink {
  final case class ToFile(path: Path, csvFormat: CSVFormat = Source.Defaults.csvFormat) extends RowSink {
    private lazy val writer: Writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    
    private[this] val anythingWritten: ValueBox[Boolean] = ValueBox(false)
    
    override def close(): Unit = {
      if(anythingWritten()) {
        writer.close()
      }
    }
    
    private val lineSeparator: String = System.lineSeparator
    
    private val renderer: Renderer = Renderer.CommonsCsv(csvFormat)
    
    override def accept(row: Row): Unit = {
      anythingWritten := true
      
      def addLineEndingIfNeeded(line: String) = if(line.endsWith(lineSeparator)) line else s"${line}${lineSeparator}"
      
      writer.write(addLineEndingIfNeeded(renderer.render(row)))
    }
  }
}
