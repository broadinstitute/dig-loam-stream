package loamstream

import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}

import scala.io.Source

/**
 * LoamStream
 * Created by oliverr on 10/8/2015.
 */
object LAPFiles {

  val charsetName: String = "UTF-8"
  val charset = Charset.forName(charsetName)

  val dir = Paths.get("pipelines/old")

  val mta = dir.resolve("simple/mta.cfg")
  val targeted = dir.resolve("real/targeted.cfg")

  val allFiles = Seq(mta, targeted)

  def asString(path: Path): String = new String(Files.readAllBytes(path), charset)

  def asSource(path: Path): Source = Source.fromFile(path.toFile, charsetName)

}
