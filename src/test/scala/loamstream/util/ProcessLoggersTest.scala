package loamstream.util

import java.nio.file.Files.exists
import java.nio.file.Path

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.io.Source

import org.scalatest.FunSuite

import loamstream.TestHelpers
import scala.sys.process.ProcessLogger

/**
 * @author clint
 * Jun 20, 2019
 */
final class ProcessLoggersTest extends FunSuite {
  test("buffering") {
    val logger = ProcessLoggers.buffering

    assert(logger.stdOut.isEmpty)
    assert(logger.stdErr.isEmpty)

    logger.out("o1")

    assert(logger.stdOut === Seq("o1"))
    assert(logger.stdErr.isEmpty)

    logger.out("o2")

    assert(logger.stdOut === Seq("o1", "o2"))
    assert(logger.stdErr.isEmpty)

    logger.err("e1")

    assert(logger.stdOut === Seq("o1", "o2"))
    assert(logger.stdErr === Seq("e1"))

    logger.err("e2")

    assert(logger.stdOut === Seq("o1", "o2"))
    assert(logger.stdErr === Seq("e1", "e2"))
  }

  test("toFilesInDir") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val logger = ProcessLoggers.toFilesInDir(workDir)

      val outFile = workDir.resolve("stdout")
      val errFile = workDir.resolve("stderr")

      assert(exists(outFile) === false)
      assert(exists(errFile) === false)

      def linesFrom(p: Path): Seq[String] = {
        CanBeClosed.enclosed(Source.fromFile(p.toFile)) { _.getLines.toIndexedSeq }
      }

      logger.out(s"o1${System.lineSeparator}")

      logger.err(s"e1${System.lineSeparator}")

      logger.out(s"o2${System.lineSeparator}")

      logger.err(s"e2${System.lineSeparator}")

      logger.close()

      assert(linesFrom(outFile) === Seq("o1", "o2"))
      assert(linesFrom(errFile) === Seq("e1", "e2"))
    }
  }

  test("PassThrough") {
    def doTest(level: LogContext.Level): Unit = {
      val logCtx = new ProcessLoggersTest.MockLogCtx

      val name = "asdf"

      val logger = new ProcessLoggers.PassThrough(name, level)(logCtx)

      logger.out(s"o1")

      logger.err(s"e1")

      logger.out(s"o2")

      logger.err(s"e2")

      val expected = Seq(
        (level, "'asdf' (via stdout): o1"),
        (level, "'asdf' (via stderr): e1"),
        (level, "'asdf' (via stdout): o2"),
        (level, "'asdf' (via stderr): e2"))

      assert(logCtx.logEvents === expected)
    }

    doTest(LogContext.Level.Debug)
    doTest(LogContext.Level.Error)
    doTest(LogContext.Level.Info)
    doTest(LogContext.Level.Trace)
    doTest(LogContext.Level.Warn)
  }

}

object ProcessLoggersTest {
  private final class MockProcessLogger extends ProcessLogger {
    val outStrings: Buffer[String] = new ArrayBuffer
    val errStrings: Buffer[String] = new ArrayBuffer
    var timesBufferInvoked: Int = 0

    override def out(s: => String): Unit = outStrings += s

    override def err(s: => String): Unit = errStrings += s

    override def buffer[T](f: => T): T = {
      timesBufferInvoked += 1

      f
    }
  }

  private final class MockLogCtx extends LogContext {
    val logEvents: Buffer[(LogContext.Level, String)] = new ArrayBuffer

    override def log(level: LogContext.Level, s: => String): Unit = logEvents += (level -> s)

    override def log(level: LogContext.Level, s: => String, e: Throwable): Unit = ???
  }
}
