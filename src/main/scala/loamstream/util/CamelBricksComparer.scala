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

    def startOfLine: Pos = copy(i = i - col, col = 0)

    def hasExhausted(string: String): Boolean = i > string.length

    def substring(string: String, length: Int = 1): String = {
      if (i > string.length) {
        ""
      } else if (i + length > string.length) {
        string.substring(i)
      } else {
        string.substring(i, i + length)
      }
    }

    def substringUntil(string: String, endPos: Pos): String = substring(string, endPos.i - i)

    def preLine(string: String): String = {
      if (i < string.length) {
        string.substring(i - col, i)
      } else if (i - col < string.length) {
        string.substring(i - col)
      } else {
        ""
      }
    }

  }

  case class Hood(pos: Pos, string: String, preLine: String)

  case class Diff(hood1: Hood, hood2: Hood)

  trait Consumer {
    def consume(text1: String, text2: String, pos1: Pos, pos2: Pos): (Pos, Pos, Seq[Diff])

    def >>(otherConsumer: Consumer): Consumer = new ComposedConsumer(this, otherConsumer)
  }

  class ComposedConsumer(val consumer1: Consumer, val consumer2: Consumer) extends Consumer {
    override def consume(text1: String, text2: String, pos1: Pos, pos2: Pos): (Pos, Pos, Seq[Diff]) = {
      val consumer1Result = consumer1.consume(text1, text2, pos1, pos2)
      val (pos1New, pos2New, diffsNew) = consumer1Result
      if (pos1 != pos1New || pos2 != pos2New || diffsNew.nonEmpty) {
        consumer1Result
      } else {
        consumer2.consume(text1, text2, pos1, pos2)
      }
    }
  }

  val whiteSpaceConsumer = new Consumer {
    val lineBreak = """\R"""
    val whitespace = """\s"""

    def consumeWhiteSpace(text: String, pos: Pos): Pos = {
      var posNew = pos
      var thereMayBeMore = true
      while (thereMayBeMore) {
        if (posNew.hasExhausted(text)) {
          thereMayBeMore = false
        } else if (posNew.substring(text, 2).matches(lineBreak)) {
          posNew = posNew.afterDoubleCharLineBreak
        } else {
          val next = posNew.substring(text)
          if (next.matches(lineBreak)) {
            posNew = posNew.afterSingleCharLineBreak
          } else if (next.matches(whitespace)) {
            posNew += 1
          } else {
            thereMayBeMore = false
          }
        }
      }
      posNew
    }

    override def consume(text1: String, text2: String, pos1: Pos, pos2: Pos): (Pos, Pos, Seq[Diff]) = {
      val posNew1 = consumeWhiteSpace(text1, pos1)
      val posNew2 = consumeWhiteSpace(text2, pos2)
      val hadLineBreaks1 = posNew1.line != pos1.line
      val hadLineBreaks2 = posNew2.line != pos2.line
      val diffs = if (hadLineBreaks1 != hadLineBreaks2) {
        val hood1 = Hood(pos1, pos1.substringUntil(text1, posNew1), pos1.preLine(text1))
        val hood2 = Hood(pos2, pos2.substringUntil(text2, posNew2), pos2.preLine(text2))
        Seq(Diff(hood1, hood2))
      } else {
        Seq.empty
      }
      (posNew1, posNew2, diffs)
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
        Seq(Diff(hood1, hood2))
      }
      (pos1 + 1, pos2 + 1, diffs)
    }
  }

  def compare(text1: String, text2: String, nDiffMax: Int = 10): Seq[Diff] = {
    var diffs: Seq[Diff] = Seq.empty
    var pos1 = Pos.start
    var pos2 = Pos.start
    val consumer = whiteSpaceConsumer >> nextCharacterConsumer
    while ((!pos1.hasExhausted(text1) || !pos2.hasExhausted(text2)) && (nDiffMax == 0 || diffs.size < nDiffMax)) {
      val (pos1New, pos2New, diffsNew) = consumer.consume(text1, text2, pos1, pos2)
      pos1 = pos1New
      pos2 = pos2New
      diffs ++= diffsNew
    }
    diffs
  }

}
