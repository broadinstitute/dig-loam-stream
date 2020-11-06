package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import java.nio.file.Files

/**
 * @author clint
 * Nov 6, 2020
 */
final class RowSinkTest extends FunSuite {
  test("ToFile.close()") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("foo")
      
      val sink = RowSink.ToFile(file)
      
      assert(Files.exists(file) === false)
      
      sink.close()
      
      assert(Files.exists(file) === false)
    }
  }
  
  test("ToFile - write some rows") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("bar")
      
      //NB: Use a non-default CsvFormat
      val sink = RowSink.ToFile(file, Source.Formats.spaceDelimited)
      
      val rows = Seq(
          LiteralRow("a", "b", "c"),
          LiteralRow("x", "y", "z"),
          LiteralRow("11", "12", "13"))
      
      assert(Files.exists(file) === false)
          
      rows.foreach(sink.accept)
          
      sink.close()
      
      assert(Files.exists(file) === true)
      
      val expected = """|a b c
                        |x y z
                        |11 12 13""".stripMargin.trim
                        
      //NB: Trim to ignore any empty-last-line differences that we don't care about
      assert(loamstream.util.Files.readFrom(file).trim === expected)
    }

  }
  
      /**
     * final case class ToFile(path: Path, csvFormat: CSVFormat = Source.Defaults.csvFormat) extends RowSink {
    private lazy val writer: Writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    
    override def close(): Unit = writer.close()
    
    private val lineSeparator: String = System.lineSeparator
    
    private val renderer: Renderer = Renderer.CommonsCsv(csvFormat)
    
    override def accept(row: Row): Unit = {
      def addLineEndingIfNeeded(line: String) = if(line.endsWith(lineSeparator)) line else s"${line}${lineSeparator}"
      
      writer.write(addLineEndingIfNeeded(renderer.render(row)))
    }
  }
     */
}
