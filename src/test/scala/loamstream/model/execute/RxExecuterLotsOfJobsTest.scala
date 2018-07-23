package loamstream.model.execute

import org.scalatest.FunSuite
import java.nio.file.Paths

import loamstream.compiler.LoamEngine
import loamstream.util.PlatformUtil
import loamstream.TestHelpers
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool
import loamstream.util.PathEnrichments
import loamstream.loam.LoamGraph
import loamstream.util.Files
import java.nio.file.Path
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.loam.LoamToolBox

/**
 * @author clint
 * Dec 9, 2016
 *
 * A test for RxExecuter's handling of "lots" (100s) of jobs.
 */
final class RxExecuterLotsOfJobsTest extends FunSuite {

  private val cancelOnWindows = true

  import PathEnrichments._
  
  test("lots of jobs don't blow the stack") {
    val outputDir: Path =  TestHelpers.getWorkDir(getClass.getSimpleName)
    
    if (cancelOnWindows && PlatformUtil.isWindows) {
      cancel("Cancelled on Windows because there it takes too long to run and hogs CPU.")
    }

    val executer = RxExecuter.defaultWith(JobFilter.RunEverything)

    val g = graph(outputDir)
    
    assert(g.finalTools.flatMap(g.toolsPreceding) === g.initialTools)
    
    assert(g.initialTools.flatMap(g.toolsSucceeding) === g.finalTools)
    
    val executable = LoamEngine.toExecutable(g)

    val numShardedJobs = chrProps.map(_.numShards).sum
    
    //The sharded jobs
    assert(executable.jobNodes.size === numShardedJobs)
    
    //Dependencies of the sharded jobs, the per-chromosome jobs
    assert(executable.jobNodes.flatMap(_.inputs).size === chrProps.size) 
    
    val expectedNumberOfJobs = {
      val numPerChromosomeJobs = chrProps.size
      
      numShardedJobs + numPerChromosomeJobs
    }

    val allJobs = ExecuterHelpers.flattenTree(executable.jobNodes)
    
    assert(allJobs.size === expectedNumberOfJobs)

    val results = try {
      executer.execute(executable)
    } finally {
      executer.stop()
    }

    assert(results.size === expectedNumberOfJobs)

    def failures = results.filter { case (job, state) => !state.isSuccess }

    assert(results.values.forall(_.isSuccess), s"Expected all jobs to succeed, but had these failures: $failures")
  }

  import RxExecuterLotsOfJobsTest.Props
  
  //Adapted from camp_chr_12_22.loam
  private val chrProps: Seq[Props] = Seq(
      Props(12, 135, 60180),
      Props(13, 97, 19020046),
      Props(14, 89, 19000016),
      Props(15, 83, 20000040),
      Props(16, 91, 60085),
      Props(17, 82, 51),
      Props(18, 79, 10082),
      Props(19, 60, 60841),
      Props(20, 64, 60342),
      Props(21, 39, 9411238),
      Props(22, 36, 16050074))
  
  //Adapted from camp_chr_12_22.loam
  private def graph(outputDir: Path): LoamGraph = TestHelpers.makeGraph { implicit context =>
    import LoamPredef._
    import LoamCmdTool._
    import PathEnrichments._
    
    // Map: Chrom Number -> (Number of Shards, Offset for Start Position)
    val input = store("src/test/resources/a.txt").asInput

    val numBasesPerShard = 1000000

    //---------------------------LOOP THROUGH CHROMOSOMES-----------------------------------------
    for (Props(chrNum, numShards, offset) <- chrProps) {
      val chr = s"chr${chrNum}"

      val chrFile = store(outputDir / s"a-$chr.txt")

      cmd"cp $input $chrFile".in(input).out(chrFile).tag(s"outer-$chr")

      //---------------------------LOOP THROUGH WINDOWS WITHIN CHROMOSOME-------------------------
      for (shard <- 0 until numShards) {
        val start = offset + (shard * numBasesPerShard) + 1
        val end = start + numBasesPerShard - 1

        val imputed = store(outputDir / s"imputed_data_${chr}_${shard}.txt")

        cmd"cp $chrFile $imputed".in(chrFile).out(imputed).tag(s"inner-$chr-$shard")
      }
    }
  }
}

object RxExecuterLotsOfJobsTest {
  private final case class Props(chromosome: Int, numShards: Int, offset: Int)
}
