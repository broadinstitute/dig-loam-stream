package loamstream.decompiler

import loamstream.camelbricks.{CamelBricksEntry, CamelBricksPipeline, CamelBricksRawEntry}

import scala.io.Source

/**
 * LoamStream
 * Created by oliverr on 10/13/2015.
 */
object CamelBricksDecompiler {

  val trailingWhitespaces = """\s+$"""
  val backslash = """\"""

  def decompile(source: Source): CamelBricksPipeline = {
    var statements: Seq[CamelBricksEntry] = Seq.empty
    val lineIter = source.getLines()
    var commandBuffer = ""
    while (lineIter.hasNext) {
      val line = lineIter.next()
      val lineNoWhiteEnd = line.replaceFirst(trailingWhitespaces, "")
      if (lineNoWhiteEnd.trim.nonEmpty) {
        if(lineNoWhiteEnd.endsWith(backslash)) {
          commandBuffer += lineNoWhiteEnd.stripSuffix(backslash)
        } else {
          statements :+= CamelBricksRawEntry(commandBuffer + lineNoWhiteEnd)
          commandBuffer = ""
        }
      } else if(commandBuffer.trim.nonEmpty){
        statements :+= CamelBricksRawEntry(commandBuffer)
      }
    }
    CamelBricksPipeline(statements)
  }

}
