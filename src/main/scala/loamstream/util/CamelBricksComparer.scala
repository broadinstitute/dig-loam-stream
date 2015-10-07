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

  def compare(text1: String, text2: String): Seq[Diff] = {
    var diffs: Seq[Diff] = Seq.empty
    var pos1 = Pos.start
    var pos2 = Pos.start
    while (!pos1.hasExhausted(text1) || !pos2.hasExhausted(text2)) {
      val letter1 = pos1.substring(text1, 1)
      val letter2 = pos2.substring(text2, 1)
      if (letter1 == letter2) {
        pos1 += 1
        pos2 += 1
      } else {
        val hood1 = Hood(pos1, letter1, pos1.preLine(text1))
        val hood2 = Hood(pos2, letter2, pos2.preLine(text2))
        diffs :+= Diff(hood1, hood2, isEquivalent = false) // TODO: test, then implement equivalency
      }
    }
    diffs
  }

}
