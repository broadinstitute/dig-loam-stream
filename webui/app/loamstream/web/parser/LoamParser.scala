package loamstream.web.parser

import loamstream.util.{Miss, Shot}
import scala.tools.nsc.interactive.Global

/**
  * LoamStream
  * Created by oliverr on 5/10/2016.
  */
object LoamParser {

  val compiler : Global = ???

  trait Result

  def parse(string: String): Shot[Result] = {
    Miss("Parser is not yet implemented!")
  }

}
