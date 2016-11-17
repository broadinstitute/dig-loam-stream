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
  
  test("escapeCommandString") {
    import CommandLineStringJob.escapeCommandString
    
    def doTestIsNoOp(s: String): Unit = {
      assert(escapeCommandString(s) === s)
    }
    
    doTestIsNoOp("")
    doTestIsNoOp("abc")
    doTestIsNoOp(" foo ")
    
    if(isWindows) {
      assert(escapeCommandString("$") === """\\\$""")
      assert(escapeCommandString("""\""") === """\\\\""")
    } else {
      doTestIsNoOp("$")
      doTestIsNoOp("""\""")
    }

    //scalastyle:off line.length
    val complex = """(head -1 ./BIOME_AFFY.kinship.pruned.king.kin0 ; sed '1d' ./BIOME_AFFY.kinship.pruned.king.kin0 | awk '{if($8 >= 0.0884) print $0}' | sort -rn -k8,8) > ./BIOME_AFFY.kinship.pruned.king.kin0.related"""

    if(isWindows) {
      val expected = """(head -1 ./BIOME_AFFY.kinship.pruned.king.kin0 ; sed '1d' ./BIOME_AFFY.kinship.pruned.king.kin0 | awk '{if(\\\\$8 >= 0.0884) print \\\\$0}' | sort -rn -k8,8) > ./BIOME_AFFY.kinship.pruned.king.kin0.related"""
      
      assert(escapeCommandString(complex) === expected)
    } else {
      doTestIsNoOp(complex)
    }
    //scalastyle:off line.length
  }
  
  test("tokensToRun") {
    import CommandLineStringJob.tokensToRun
    
    def shDashC(s: String): Seq[String] = Seq("sh", "-c", s)
    
    def doTestNoEscaping(s: String): Unit = {
      assert(tokensToRun(s) === shDashC(s))
    }
    
    doTestNoEscaping("")
    doTestNoEscaping("abc")
    doTestNoEscaping(" foo ")
    
    if(isWindows) {
      assert(tokensToRun("$") === shDashC("""\\\$"""))
      assert(tokensToRun("""\""") === shDashC("""\\\\"""))
    } else {
       doTestNoEscaping("$")
       doTestNoEscaping("""\""")
    }

    //scalastyle:off line.length
    val complex = """(head -1 ./BIOME_AFFY.kinship.pruned.king.kin0 ; sed '1d' ./BIOME_AFFY.kinship.pruned.king.kin0 | awk '{if($8 >= 0.0884) print $0}' | sort -rn -k8,8) > ./BIOME_AFFY.kinship.pruned.king.kin0.related"""
    
    if(isWindows) {
      val expected = """(head -1 ./BIOME_AFFY.kinship.pruned.king.kin0 ; sed '1d' ./BIOME_AFFY.kinship.pruned.king.kin0 | awk '{if(\\\\$8 >= 0.0884) print \\\\$0}' | sort -rn -k8,8) > ./BIOME_AFFY.kinship.pruned.king.kin0.related"""
      
      assert(tokensToRun(complex) === shDashC(expected))
    } else {
      doTestNoEscaping(complex)
    }

    //scalastyle:off line.length
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
      source.getLines.map(_.trim).filter(_.nonEmpty).size
    }
    
    assert(numLines === 11) 
  }
  //scalastyle:on magic.number
}