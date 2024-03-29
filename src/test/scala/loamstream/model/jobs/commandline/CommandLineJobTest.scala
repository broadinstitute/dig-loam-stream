package loamstream.model.jobs.commandline

import loamstream.util.CanBeClosed
import org.scalatest.FunSuite

import scala.io.Source
import loamstream.TestHelpers
import loamstream.loam.LoamGraph
import loamstream.loam.LoamCmdTool
import loamstream.compiler.LoamPredef

/**
  * @author clint
  *         Nov 16, 2016
  */
final class CommandLineJobTest extends FunSuite {
  test("Complex command that needs escaping") {
    val outputPath = TestHelpers.getWorkDir(getClass.getSimpleName).resolve("foo")
    
    val graph: LoamGraph = TestHelpers.makeGraph { implicit sc =>
      import loamstream.loam.LoamSyntax._
    
      val input = store("src/test/resources/test-data-CommandLineStringJobTest").asInput
      val output = store(outputPath)

      cmd"(head -1 $input ; sed '1d' $input | awk '{if($$8 >= 0.0884) print $$0}') > $output"
    }
    
    val jobResults = TestHelpers.run(graph)

    assert(jobResults.values.head.isSuccess)

    assert(jobResults.size === 1)

    val numLines = CanBeClosed.enclosed(Source.fromFile(outputPath.toFile)) { source =>
      source.getLines.map(_.trim).count(_.nonEmpty)
    }

    assert(numLines === 11)
  }
}
