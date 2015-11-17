package loamstream.decompiler

import java.nio.file.Path

import loamstream.LAPFiles
import loamstream.lap.LAPPipeline
import util.shot.{Hit, Miss, Shot}

/**
  * LoamStream
  * Created by oliverr on 10/13/2015.
  */
object LAPParseApp extends App {

  def parse(path: Path): Shot[LAPPipeline] = LAPParser.parse(LAPFiles.asSource(path))

  val pipelineShot = parse(LAPFiles.targeted)

  pipelineShot match {
    case Hit(pipeline) =>
      for (entry <- pipeline.entries) {
        println(entry.asString)
      }
    case Miss(snag) =>
      println(snag)
  }

}
