package loamstream

import org.scalatest.FunSuite
import loamstream.util.Paths
import java.nio.file.Files.exists
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.DataHandle
import loamstream.model.execute.Executable
import loamstream.model.execute.RxExecuter
import loamstream.util.Files.writeTo
import loamstream.util.Files.readFrom
import loamstream.util.FileMonitor
import loamstream.loam.LoamToolBox

/**
 * @author clint
 * Dec 11, 2018
 */
final class DownstreamJobsDontRunWhenOutputsMissingTest extends FunSuite {
  
  test("A job that succeeds, but whose inputs don't appear in time, shouldn't lead to downstream jobs being run") {
    
    import Paths.Implicits.PathHelpers
    
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val inputFile = workDir / "in.txt"
    val intermediateFile = workDir / "inter.txt"
    val outputFile = workDir / "out.txt"
    
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val input = store(inputFile).asInput
      val intermediate = store(intermediateFile)
      val output = store(outputFile)
      
      cmd"echo foo".in(input).out(intermediate).tag("first")
      cmd"echo bar".in(intermediate).out(output).tag("second")
    }
    
    assert(!exists(inputFile))
    assert(!exists(intermediateFile))
    assert(!exists(outputFile))
    
    writeTo(inputFile)("foo")
    
    assert(exists(inputFile))
    assert(!exists(intermediateFile))
    assert(!exists(outputFile))
    
    val executable = (new LoamToolBox).createExecutable(graph)
        
    val firstJob = executable.jobNodes.head.dependencies.head.job
    val secondJob = executable.jobNodes.head.job
    
    assert(firstJob.name === "first")
    assert(secondJob.name === "second")
    
    import scala.concurrent.duration._
    
    val fileMonitor = new FileMonitor(pollingRateInHz = 1.0, maxWaitTime = 5.seconds)
    
    val executor = { 
      val default = RxExecuter.default
      
      default.copy(fileMonitor = fileMonitor, maxRunsPerJob = 1)(default.executionContext)
    }
    
    val results = executor.execute(executable)
    
    assert(exists(inputFile))
    assert(!exists(intermediateFile))
    assert(!exists(outputFile))
    
    assert(results.keySet === Set(firstJob))
    
    assert(results(firstJob).isFailure)
  }
}
