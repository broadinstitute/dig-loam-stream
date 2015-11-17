package loamstream.decompiler

import java.nio.file.Path

import loamstream.LAPFiles
import loamstream.lap.LAPPipeline

/**
 * LoamStream
 * Created by oliverr on 10/13/2015.
 */
object LAPParseApp extends App {

  def decompile(path: Path): LAPPipeline = LAPParser.decompile(LAPFiles.asSource(path))

  val pipeline = decompile(LAPFiles.targeted)

  for (entry <- pipeline.entries) {
    println(entry.asString)
  }

}
