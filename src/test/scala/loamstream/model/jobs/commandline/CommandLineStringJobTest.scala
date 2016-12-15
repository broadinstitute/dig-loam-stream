package loamstream.model.jobs.commandline

import loamstream.compiler.LoamEngine
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.util.{LoamFileUtils, Loggable}
import org.scalatest.FunSuite

import scala.io.Source

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
      val input = store[TXT].from("src/test/resources/test-data-CommandLineStringJobTest")
      val output = store[TXT].to("""" + outputPath +
        """")

      cmd"(head -1 $input ; sed '1d' $input | awk '{if($$8 >= 0.0884) print $$0}' | sort -rn -k8,8) > $output"
        """

    val logger = new Loggable {}

    val outMessageSink = LoggableOutMessageSink(logger)

    val loamEngine = LoamEngine.default(outMessageSink)

    val results = loamEngine.run(code)

    val jobResults = results.jobResultsOpt.get

    assert(jobResults.values.head.isSuccess)

    assert(jobResults.size === 1)

    val numLines = LoamFileUtils.enclosed(Source.fromFile(outputPath)) { source =>
      source.getLines.map(_.trim).count(_.nonEmpty)
    }

    assert(numLines === 11)
  }
  //scalastyle:on magic.number
}