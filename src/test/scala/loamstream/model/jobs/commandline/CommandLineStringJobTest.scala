package loamstream.model.jobs.commandline

import org.scalatest.FunSuite
import loamstream.compiler.LoamEngine
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.util.Loggable
import loamstream.util.LoamFileUtils
import scala.io.Source
import loamstream.util.PlatformUtil

/**
 * @author clint
 * Nov 16, 2016
 */
final class CommandLineStringJobTest extends FunSuite {
  
  //scalastyle:off magic.number
  
  private val isWindows = PlatformUtil.isWindows
  
  test("tokensToRun") {
    import CommandLineStringJob.tokensToRun
    
    def doTestNoEscaping(s: String): Unit = {
      val tokens = tokensToRun(s)
      assert(tokens.head === "sh")
      assert(tokens.size === 2)
    }
    
    doTestNoEscaping("")
    doTestNoEscaping("abc")
    doTestNoEscaping(" foo ")

  }
  
  test("Complex command that needs escaping") {
    val outputPath = "target/foo"
    
    val code = """
      val input = store[TXT].from("src/test/resources/test-data-CommandLineStringJobTest")
      val output = store[TXT].to("""" + outputPath + """")
      
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