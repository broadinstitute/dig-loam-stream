package loamstream.parser

import loamstream.lap.{LAPEntry, LAPPipeline, LAPRawEntry}
import util.shot.{Hit, Shot}

import scala.io.Source

/**
  * LoamStream
  * Created by oliverr on 10/13/2015.
  */
object LAPParser {

  val trailingWhitespaces = """\s+$"""
  val backslash = """\"""

  def parse(source: Source): Shot[LAPPipeline] = {
    var statements: Seq[LAPEntry] = Seq.empty
    val lineIter = source.getLines()
    var commandBuffer = ""
    while (lineIter.hasNext) {
      val line = lineIter.next()
      val lineNoWhiteEnd = line.replaceFirst(trailingWhitespaces, "")
      if (lineNoWhiteEnd.trim.nonEmpty) {
        if (lineNoWhiteEnd.endsWith(backslash)) {
          commandBuffer += lineNoWhiteEnd.stripSuffix(backslash)
        } else {
          statements :+= LAPRawEntry(commandBuffer + lineNoWhiteEnd)
          commandBuffer = ""
        }
      } else if (commandBuffer.trim.nonEmpty) {
        statements :+= LAPRawEntry(commandBuffer)
      }
    }
    Hit(LAPPipeline(statements))
  }

}
