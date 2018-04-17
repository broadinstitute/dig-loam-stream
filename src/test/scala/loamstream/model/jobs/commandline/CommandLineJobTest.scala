package loamstream.model.jobs.commandline

import loamstream.util.CanBeClosed
import org.scalatest.FunSuite

import scala.io.Source
import loamstream.TestHelpers
import loamstream.loam.LoamGraph
import loamstream.loam.LoamCmdTool
import loamstream.compiler.{LoamEngine, LoamPredef}

/**
  * @author clint
  *         Nov 16, 2016
  */
final class CommandLineJobTest extends FunSuite {
  test("Complex command that needs escaping") {
    val outputPath = TestHelpers.getWorkDir(getClass.getSimpleName).resolve("foo")
    
    val graph: LoamGraph = TestHelpers.makeGraph { implicit sc =>
      import LoamPredef._
      import LoamCmdTool._
    
      val input = store.at("src/test/resources/test-data-CommandLineStringJobTest").asInput
      val output = store.at(outputPath)

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

  test("Without Docker location") {
    val graph = TestHelpers.makeGraph { implicit sc =>
      import LoamCmdTool._
      cmd"has no docker location"
    }
    assert(graph.tools.size === 1)
    assert(graph.dockerLocations.isEmpty)
    val executable = LoamEngine.toExecutable(graph)
    val jobs = executable.jobNodes
    assert(jobs.size === 1)
    val job = jobs.head
    assert(job.isInstanceOf[CommandLineJob])
    assert(job.asInstanceOf[CommandLineJob].dockerLocationOpt === None)
  }

  test("With Docker location") {
    val dockerLocation = "abc:xyz"
    val graph = TestHelpers.makeGraph { implicit sc =>
      import LoamCmdTool._
      cmd"has no docker location".withDockerLocation(dockerLocation)
    }
    assert(graph.tools.size === 1)
    assert(graph.dockerLocations.size === 1)
    assert(graph.dockerLocations.values.head === dockerLocation)
    val executable = LoamEngine.toExecutable(graph)
    val jobs = executable.jobNodes
    assert(jobs.size === 1)
    val job = jobs.head
    assert(job.isInstanceOf[CommandLineJob])
    assert(job.asInstanceOf[CommandLineJob].dockerLocationOpt === Some(dockerLocation))
  }
}
