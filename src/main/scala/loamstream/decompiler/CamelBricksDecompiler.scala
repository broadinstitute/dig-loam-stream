package loamstream.decompiler

import loamstream.camelbricks.{CamelBricksPipeline, CamelBricksRawEntry, CamelBricksEntry}

import scala.io.Source

/**
 * LoamStream
 * Created by oliverr on 10/13/2015.
 */
object CamelBricksDecompiler {

  def decompile(source: Source): CamelBricksPipeline = {
    var statements: Seq[CamelBricksEntry] = Seq.empty
    for (line <- source.getLines()) {
      statements :+= CamelBricksRawEntry(line)
    }
    CamelBricksPipeline(statements)
  }

}
