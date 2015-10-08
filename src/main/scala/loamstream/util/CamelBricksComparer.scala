package loamstream.util

/**
 * LoamStream
 * Created by oliverr on 10/7/2015.
 */
object CamelBricksComparer {

  object Pos {
    def start: Pos = Pos(0, 0, 0)
  }

  case class Pos(i: Int, line: Int, col: Int) {
    def +(j: Int): Pos = copy(i = i + j, col = col + j)

    def afterDoubleCharLineBreak: Pos = copy(i = i + 2, line = line + 1, col = 0)

    def afterSingleCharLineBreak: Pos = copy(i = i + 1, line = line + 1, col = 0)

    def lineFed: Pos = copy(line = line + 1, col = 0)

    def startOfLine: Pos = copy(i = i - col, col = 0)

    def hasExhausted(string: String): Boolean = i > string.length

    def substring(string: String, length: Int = 1) = {
      if (i > string.length) {
        ""
      } else if (i + length > string.length) {
        string.substring(i)
      } else {
        string.substring(i, i + length)
      }
    }

    def preLine(string: String): String = string.substring(i - col, i)

  }

  case class Hood(pos: Pos, string: String, preLine: String)

  case class Diff(hood1: Hood, hood2: Hood, isEquivalent: Boolean)

  trait Consumer {
    def consume(text1: String, text2: String, pos1: Pos, pos2: Pos): (Pos, Pos, Seq[Diff])

    def >>(otherConsumer: Consumer): Consumer = new ComposedConsumer(this, otherConsumer)
  }

  class ComposedConsumer(val consumer1: Consumer, val consumer2: Consumer) extends Consumer {
    override def consume(text1: String, text2: String, pos1: Pos, pos2: Pos): (Pos, Pos, Seq[Diff]) = {
      val consumer1Result = consumer1.consume(text1, text2, pos1, pos2)
      val (pos1New, pos2New, diffsNew) = consumer1Result
      if (pos1 != pos1New || pos2 != pos2New && diffsNew.nonEmpty) {
        consumer1Result
      } else {
        consumer2.consume(text1, text2, pos1, pos2)
      }
    }
  }

  val lineBreakConsumer = new Consumer {
    val lineBreak = """\R"""
    val whitespace = """\s"""

    def consumeWhiteSpace(text: String, pos: Pos): Pos = {
      var posNew = pos
      var thereMayBeMore = true
      while (thereMayBeMore) {
        if (pos.hasExhausted(text)) {
          thereMayBeMore = false
        } else if (pos.substring(text, 2).matches(lineBreak)) {
          posNew = posNew.afterDoubleCharLineBreak
        } else {
          val next = pos.substring(text)
          if(next.matches(lineBreak)) {
            posNew = posNew.afterSingleCharLineBreak
          } else if(next.matches(whitespace)) {
            posNew += 1
          } else {
            thereMayBeMore = false
          }
        }
      }
      posNew
    }

    override def consume(text1: String, text2: String, pos1: Pos, pos2: Pos): (Pos, Pos, Seq[Diff]) = {
      ???
    }
  }

  val nextCharacterConsumer = new Consumer {
    override def consume(text1: String, text2: String, pos1: Pos, pos2: Pos): (Pos, Pos, Seq[Diff]) = {
      val char1 = pos1.substring(text1, 1)
      val char2 = pos2.substring(text2, 1)
      val diffs = if (char1 == char2) {
        Seq.empty[Diff]
      } else {
        val hood1 = Hood(pos1, char1, pos1.preLine(text1))
        val hood2 = Hood(pos2, char2, pos2.preLine(text2))
        Seq(Diff(hood1, hood2, isEquivalent = false))
      }
      (pos1 + 1, pos2 + 1, diffs)
    }
  }

  def compare(text1: String, text2: String, nDiffMax: Int = 10): Seq[Diff] = {
    var diffs: Seq[Diff] = Seq.empty
    var pos1 = Pos.start
    var pos2 = Pos.start
    while ((!pos1.hasExhausted(text1) || !pos2.hasExhausted(text2)) && (nDiffMax == 0 || diffs.size < nDiffMax)) {
      val (pos1New, pos2New, diffsNew) = nextCharacterConsumer.consume(text1, text2, pos1, pos2)
      pos1 = pos1New
      pos2 = pos2New
      diffs ++= diffsNew
    }
    diffs
  }

}
