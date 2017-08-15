package loamstream.model.jobs.commandline

import loamstream.compiler.LoamEngine
import loamstream.util.{LoamFileUtils, Loggable}
import org.scalatest.FunSuite

import scala.io.Source
import loamstream.TestHelpers
import loamstream.loam.LoamGraph
import loamstream.loam.LoamCmdTool
import loamstream.compiler.LoamPredef
import loamstream.loam.ops.StoreType
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamProjectContext

/**
  * @author clint
  *         Nov 16, 2016
  */
final class CommandLineStringJobTest extends FunSuite {

  //scalastyle:off magic.number

  test("Complex command that needs escaping") {
    val outputPath = "target/foo"
    
    val graph: LoamGraph = TestHelpers.makeGraph { implicit sc =>
      import LoamPredef._
      import LoamCmdTool._
      import StoreType._
    
      val input = store[TXT].at("src/test/resources/test-data-CommandLineStringJobTest").asInput
      val output = store[TXT].at(outputPath)

      cmd"(head -1 $input ; sed '1d' $input | awk '{if($$8 >= 0.0884) print $$0}') > $output"
    }
    
    val jobResults = TestHelpers.run(graph)

    assert(jobResults.values.head.isSuccess)

    assert(jobResults.size === 1)

    val numLines = LoamFileUtils.enclosed(Source.fromFile(outputPath)) { source =>
      source.getLines.map(_.trim).count(_.nonEmpty)
    }

    assert(numLines === 11)
  }
  //scalastyle:on magic.number
}
