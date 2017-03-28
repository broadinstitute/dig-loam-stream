package loamstream.model.jobs.commandline

import loamstream.compiler.LoamEngine
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.util.{LoamFileUtils, Loggable}
import org.scalatest.FunSuite

import scala.io.Source
import loamstream.TestHelpers

/**
  * @author clint
  *         Nov 16, 2016
  */
final class CommandLineStringJobTest extends FunSuite {

  //scalastyle:off magic.number

  test("Complex command that needs escaping") {
    val outputPath = "target/foo"

    val code =
      """
      val input = store[TXT].at("src/test/resources/test-data-CommandLineStringJobTest").asInput
      val output = store[TXT].at("""" + outputPath +
        """")

      cmd"(head -1 $input ; sed '1d' $input | awk '{if($$8 >= 0.0884) print $$0}') > $output"
        """

    val logger = new Loggable {}

    val outMessageSink = LoggableOutMessageSink(logger)

    val loamEngine = LoamEngine.default(TestHelpers.config, outMessageSink)

    val results = loamEngine.run(code)

    val jobResults = results.jobExecutionsOpt.get

    assert(jobResults.values.head.isSuccess)

    assert(jobResults.size === 1)

    val numLines = LoamFileUtils.enclosed(Source.fromFile(outputPath)) { source =>
      source.getLines.map(_.trim).count(_.nonEmpty)
    }

    assert(numLines === 11)
  }
  //scalastyle:on magic.number
}
