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
      
      val sink = RowSink.ToFile(file, RowSink.Renderers.json)
      
      assert(Files.exists(file) === false)
      
      sink.close()
      
      assert(Files.exists(file) === false)
    }
  }
  
  test("ToFile - write some rows") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("bar")
      
      //NB: Use a non-default CsvFormat
      val sink = RowSink.ToFile(file, RowSink.Renderers.csv(Source.Formats.spaceDelimited))
      
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
}
