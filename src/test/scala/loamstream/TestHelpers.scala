package loamstream

import java.io.ByteArrayOutputStream
import java.nio.file.{Path, Paths}

/**
  * @author clint
  *         date: Mar 10, 2016
  */
object TestHelpers {
  def path(p: String): Path = Paths.get(p)

  val approxDoublePrecision = 1e-16
  val graceFactor = 20
  val tolerance = graceFactor * approxDoublePrecision

  def areWithinExpectedError(x: Double, y: Double): Boolean = (x - y) / Math.max(x.abs, y.abs) < tolerance

  /**
   * Taken from [[https://github.com/scallop/scallop Scallop]]
   */
  def captureOutput(f: => Unit): (String, String) = {
    val normalOut = Console.out
    val normalErr = Console.err
    val streamOut = new ByteArrayOutputStream()
    val streamErr = new ByteArrayOutputStream()
    Console.withOut(streamOut) {
      Console.withErr(streamErr) {
        f
      }
    }

    (streamOut.toString, streamErr.toString)
  }
}