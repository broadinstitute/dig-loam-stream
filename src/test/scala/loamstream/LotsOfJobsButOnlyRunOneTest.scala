package loamstream

import org.scalatest.FunSuite
import loamstream.util.Paths
import TestHelpers.path
import loamstream.compiler.LoamEngine
import loamstream.apps.Main
import loamstream.model.execute.ByNameJobFilter
import loamstream.model.execute.DryRunner

/**
 * @author clint
 * Nov 18, 2019
 * 
 */
final class LotsOfJobsButOnlyRunOneTest extends FunSuite {
  test("Big linear pipeline, only run one job in the middle") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      import Paths.Implicits._
      
      val startFile = (workDir / "A.txt")
      
      val n = 1000
      
      val graph = TestHelpers.makeGraph { implicit scriptContext =>
        import loamstream.loam.LoamSyntax._
        
        val start = store(startFile).asInput
        
        val one = store(workDir / "1.txt")
        
        val startToOne = cmd"cp $start $one".in(start).out(one).tag("1")
        
        val z: (Int, Store) = (2, one)
        
        (2 to n).map(i => store(workDir / s"${i}.txt")).foldLeft(z) { (acc, output) =>
          val (i, input) = acc
          
          val tool = cmd"cp $input $output".in(input).out(output).tag(i.toString)
          
          (i + 1, output)
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
      
      val jobFilter = ByNameJobFilter.allOf(s"^${n - 1}$$".r)
      
      println(s"Total jobs: ${executable.allJobs.size}")
      
      val toBeRun = DryRunner.toBeRun(jobFilter, executable)
      
      println(s"Jobs to be run: ${toBeRun.size}")
      
      toBeRun.foreach(println)
    }
  }
}
