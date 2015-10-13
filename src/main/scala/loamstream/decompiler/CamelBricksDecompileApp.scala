package loamstream.decompiler

import java.nio.file.Path

import loamstream.CamelBricksFiles
import loamstream.camelbricks.CamelBricksPipeline

/**
 * LoamStream
 * Created by oliverr on 10/13/2015.
 */
object CamelBricksDecompileApp extends App {

  def decompile(path: Path): CamelBricksPipeline = CamelBricksDecompiler.decompile(CamelBricksFiles.asSource(path))

  val pipeline = decompile(CamelBricksFiles.targeted)

  for (entry <- pipeline.entries) {
    println(entry.asString)
  }

}
