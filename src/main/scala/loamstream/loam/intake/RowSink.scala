package loamstream.loam.intake

import java.io.Closeable
import java.io.Writer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

import org.apache.commons.csv.CSVFormat

/**
 * @author clint
 * Oct 13, 2020
 */
trait RowSink[-R] extends Closeable {
  def accept(row: R): Unit
}

object RowSink {
  object Renderers {
    def csv(csvFormat: CSVFormat): RenderableRow => String = {
      val renderer = Renderer.CommonsCsv(csvFormat)
      
      row => renderer.render(row)
    }
    
    def json: RenderableJsonRow => String = { row =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      
      val obj = JObject(row.jsonValues.toList)
      
      compact(render(obj))
    }
  }
  
  final case class ToFile[R <: RenderableRow](
      path: Path,
      renderRow: R => String) extends RowSink[R] {
    private lazy val writer: Writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    
    @volatile private[this] var anythingWritten: Boolean = false
    
    override def close(): Unit = {
      if(anythingWritten) {
        writer.close()
      }
    }
    
    private val lineSeparator: String = System.lineSeparator
    
    override def accept(row: R): Unit = {
      anythingWritten = true
      
      def addLineEndingIfNeeded(line: String) = if(line.endsWith(lineSeparator)) line else s"${line}${lineSeparator}"
      
      writer.write(addLineEndingIfNeeded(renderRow(row)))
    }
  }
}
