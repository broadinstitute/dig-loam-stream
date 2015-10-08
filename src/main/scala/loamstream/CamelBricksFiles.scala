package loamstream

import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}

/**
 * LoamStream
 * Created by oliverr on 10/8/2015.
 */
object CamelBricksFiles {

  val charset = Charset.forName("UTF-8")

  val dir = Paths.get("pipelines/old")
  val mta = dir.resolve("simple/mta.cfg")
  val targeted = dir.resolve("real/targeted.cfg")

  def asString(path: Path): String = new String(Files.readAllBytes(path), charset)

  def mtaAsString: String = asString(mta)

  def targetedAsString: String = asString(targeted)

}
