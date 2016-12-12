package loamstream.model.execute

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.compiler.LoamEngine
import loamstream.compiler.messages.ClientMessageHandler

/**
 * @author clint
 * Dec 9, 2016
 */
final class RxExecuterLotsOfJobsTest extends FunSuite {
  // scalastyle:off magic.number
  
  test("lots of jobs don't blow the stack") {
    val outputDir = Paths.get("target/many-files").toFile
    
    outputDir.mkdir()
    
    assert(outputDir.exists)
    
    val executer = RxExecuter.defaultWith(JobFilter.RunEverything)
    
    val engine = LoamEngine.default(ClientMessageHandler.OutMessageSink.NoOp).copy(executer = executer)
    
    val executable = engine.compileToExecutable(code).get
    
    val expectedNumberOfJobs = 867
    
    assert(ExecuterHelpers.flattenTree(executable.jobs).size === expectedNumberOfJobs)
    
    val results = executer.execute(executable)
    
    assert(results.size === expectedNumberOfJobs)
    
    def failures = results.filter { case (job, state) => !state.isSuccess }
    
    assert(results.values.forall(_.isSuccess), s"Expected all jobs to succeed, but had these failures: $failures")
  }
  
  //Adapted from camp_chr_12_22.loam
  private val code = """
// Map: Chrom Number -> (Number of Shards, Offset for Start Position)
val input = store[TXT].from("src/test/resources/a.txt")

val outputDir = path("target/many-files")

val numBasesPerShard = 1000000

val chrProps: Map[Int, (Int, Int)] = Map(
1 -> (250, 10176),
2 -> (244, 10178),
3 -> (198, 60068),
4 -> (192, 10004),
5 -> (182, 10042),
6 -> (172, 63853),
7 -> (160, 14807),
8 -> (147, 11739),
9 -> (142, 10162),
10 -> (137, 60493),
11 -> (136, 61394),
12 -> (135, 60180),
13 -> (97, 19020046),
14 -> (89, 19000016),
15 -> (83, 20000040),
16 -> (91, 60085),
17 -> (82, 51),
18 -> (79, 10082),
19 -> (60, 60841),
20 -> (64, 60342),
21 -> (39, 9411238),
22 -> (36, 16050074))

//---------------------------LOOP THROUGH CHROMOSOMES-------------------------------------------------------------------
for (chrNum <- 12 to 22) {
  val chr = s"chr${chrNum}"

  val chrFile = store[TXT].to(outputDir / s"a-$chr.txt")

  cmd"cp $input $chrFile"

  val (numShards, offset) = chrProps(chrNum)

  //---------------------------LOOP THROUGH WINDOWS WITHIN CHROMOSOME-----------------------------------------------------
  for(shard <- 0 until numShards) {
    val start = offset + (shard * numBasesPerShard) + 1
    val end = start + numBasesPerShard - 1

    val imputed = store[TXT].to(outputDir / s"imputed_data_${chr}_bp${start}-${end}.txt")

    cmd"cp $chrFile $imputed"
  }
}
"""
  // scalastyle:on magic.number
}
