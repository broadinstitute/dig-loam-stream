package loamstream.compiler.metaapps

import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.{Files, Path}

import scala.io.Source

/**
 * LoamStream
 * Created by oliverr on 10/7/2015.
 */
object FileChunkWrapper {

  val nLinesMaxDefault = 300
  val charsetName = "UTF-8"
  val charset = Charset.forName(charsetName)

  case class FileStrings(fileName: String, prefix: String, suffix: String)

  case class FileStringsTemplate(fileName: String, prefix: String, suffix: String, tag: String, nDigits: Int = 2)
    extends (Int => FileStrings) {

    private def subs(string: String, i: Int): String = {
      val iString = "" + i
      val digits = "0" * (nDigits - iString.length) + iString
      string.replaceAll(tag, digits)
    }

    override def apply(i: Int): FileStrings = FileStrings(subs(fileName, i), subs(prefix, i), subs(suffix, i))

  }

  def wrap(inFile: Path, outDir: Path, stringsFunc: Int => FileStrings, nLinesMax: Int = nLinesMaxDefault): Unit = {
    println("Reading file " + inFile)
    val lines = Source.fromFile(inFile.toFile, charsetName).getLines
    val chunks = lines.map(_ + "\n").grouped(nLinesMax).map(_.fold("")(_ + _))
    for ((chunk, iChunk) <- chunks.zipWithIndex) {
      val strings = stringsFunc(iChunk)
      val file = outDir.resolve(strings.fileName)
      val content = strings.prefix + chunk + strings.suffix
      try {
        val writer = Files.newBufferedWriter(file, charset)
        writer.write(content)
        writer.close();
      } catch {
        case ex: IOException => ex.printStackTrace()
      }
    }
  }

}
