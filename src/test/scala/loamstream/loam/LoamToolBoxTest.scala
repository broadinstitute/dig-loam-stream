package loamstream.loam

import java.nio.file.{Files => JFiles}
import java.nio.file.Path
import java.nio.file.Paths

import scala.util.Try
import org.scalatest.FunSuite
import loamstream.compiler.LoamCompiler
import loamstream.loam.LoamToolBoxTest.Results
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.loam.ast.LoamGraphAstMapping
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.{JobResult, LJob}
import loamstream.util.Files
import loamstream.TestHelpers
import loamstream.model.jobs.commandline.CommandLineJob

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBoxTest extends FunSuite {

  private def run(script: LoamScript): Results = {

    val compiler = new LoamCompiler

    val compileResults = compiler.compile(TestHelpers.config, script)

    assert(compileResults.errors == Nil)

    val context = compileResults.contextOpt.get

    val graph = context.graph

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = new LoamToolBox(context)

    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)

    val jobResults = RxExecuter.default.execute(executable)
    
    Results(graph, mapping, jobResults)
  }

  test("Simple toy pipeline using cp.") {
    //Make files in target/ so we don't risk cluttering up the project root directory if anything goes wrong.
    withFiles("target/fileIn.txt", "target/fileOut1.txt", "target/fileOut2.txt", "target/fileOut3.txt") { paths =>
      val Seq(fileIn, fileOut1, fileOut2, fileOut3) = paths

      Files.writeTo(fileIn)("Hello World!")

      val code = LoamToolBoxTest.Sources.toyCp

      val results = run(code)

      assert(results.jobResults.size === 5, s"${results.jobResults}")
      assert(results.mapping.rootAsts.size === 3)
      assert(results.mapping.rootTools.size === 3)
      assert(results.allJobResultsAreSuccess)

      assert(JFiles.exists(fileIn))
      assert(JFiles.exists(fileOut1))
      assert(JFiles.exists(fileOut2))
      assert(JFiles.exists(fileOut3))
    }
  }

  test("'real' pipeline: 3 impute2 invocations that all depend on the same shapeit invocation should produce " +
    "expected ASTs and job graph") {

    val source = LoamToolBoxTest.Sources.imputeParallel

    val compiler = new LoamCompiler

    val compileResults = compiler.compile(TestHelpers.config, source)

    assert(compileResults.errors === Nil)

    val context = compileResults.contextOpt.get

    val graph = context.graph

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = new LoamToolBox(context)

    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)

    assert(mapping.toolAsts.size === 4)
    assert(mapping.rootAsts.size === 3)

    def getRootAst(i: Int) = mapping.rootAsts.toSeq.apply(i)

    assert(getRootAst(0).dependencies.size === 1)
    assert(getRootAst(1).dependencies.size === 1)
    assert(getRootAst(2).dependencies.size === 1)

    val dep0 = getRootAst(0).dependencies.head.producer
    val dep1 = getRootAst(1).dependencies.head.producer
    val dep2 = getRootAst(2).dependencies.head.producer

    assert(dep0 eq dep1)
    assert(dep1 eq dep2)
    assert(dep0 eq dep2)

    assert(executable.jobs.size === 3)

    val Seq(job0, job1, job2) = executable.jobs.toSeq.map(_.asInstanceOf[CommandLineJob])

    assert(job0.commandLineString.contains("impute2"))
    assert(job1.commandLineString.contains("impute2"))
    assert(job2.commandLineString.contains("impute2"))
    
    assert(job0.inputs.size === 1)
    assert(job1.inputs.size === 1)
    assert(job2.inputs.size === 1)

    val depJob0 = job0.inputs.head.asInstanceOf[CommandLineJob]
    val depJob1 = job1.inputs.head.asInstanceOf[CommandLineJob]
    val depJob2 = job2.inputs.head.asInstanceOf[CommandLineJob]

    assert(depJob0.commandLineString.contains("shapeit"))
    assert(depJob1.commandLineString.contains("shapeit"))
    assert(depJob2.commandLineString.contains("shapeit"))
    
    assert(depJob0.inputs === Set.empty)
    assert(depJob1.inputs === Set.empty)
    assert(depJob2.inputs === Set.empty)
    
    //All 3 dependency jobs (shapeit jobs) should be the same
    assert(depJob0 eq depJob1)
    assert(depJob1 eq depJob2)
    assert(depJob0 eq depJob2)
  }

  private def withFiles[A](names: String*)(f: Seq[Path] => A): A = {
    //TODO: use Commons-IO, like elsewhere 
    def deleteQuietly(p: Path): Unit = Try(JFiles.delete(p))

    val paths = names.map(Paths.get(_))

    paths.foreach(deleteQuietly)

    try {
      f(paths)
    }
    finally {
      paths.foreach(deleteQuietly)
    }
  }
}

object LoamToolBoxTest {

  final case class Results(graph: LoamGraph, mapping: LoamGraphAstMapping, jobResults: Map[LJob, JobResult]) {
    
    def allJobResultsAreSuccess: Boolean = jobResults.values.forall(_.isSuccess)
    
  }

  object Sources {
    val toyCp = LoamScript("ToyCp", {
      """
        |val fileIn = store[TXT].at(path("target/fileIn.txt")).asInput
        |val fileTmp1 = store[TXT]
        |val fileTmp2 = store[TXT]
        |val fileOut1 = store[TXT].at(path("target/fileOut1.txt"))
        |val fileOut2 = store[TXT].at(path("target/fileOut2.txt"))
        |val fileOut3 = store[TXT].at(path("target/fileOut3.txt"))
        |cmd"cp $fileIn $fileTmp1"
        |cmd"cp $fileTmp1 $fileTmp2"
        |cmd"cp $fileTmp2 $fileOut1"
        |cmd"cp $fileTmp2 $fileOut2"
        |cmd"cp $fileTmp2 $fileOut3"
      """.stripMargin
    })

    // scalastyle:off line.size.limit
    val imputeParallel = LoamScript("ImputeParallel", {
      """
val kgpDir = path("/humgen/diabetes/users/ryank/internal_qc/1kg_phase3/1000GP_Phase3")
val softDir = path("/humgen/diabetes/users/ryank/software")

val shapeit = softDir / "shapeit/bin/shapeit"
val impute2 = softDir / "impute_v2.3.2_x86_64_static/impute2"

val chr = 22
val nShards = 3
val offset = 20400000
val basesPerShard = 100000

val homeDir = path("/home/unix/cgilbert")
val dataDir = homeDir / "imputation"
val shapeitDataDir = dataDir / "shapeit_example"
val impute2DataDir = dataDir / "impute2_example"

val outputDir = homeDir / "output"

val data = store[VCF].at(shapeitDataDir / "gwas.vcf.gz").asInput
val geneticMap = store[TXT].at(shapeitDataDir / "genetic_map.txt.gz").asInput
val phasedHaps = store[TXT].at(outputDir / "phased.haps.gz")
val phasedSamples = store[TXT].at(outputDir / "phased.samples.gz")
val log = store[TXT].at(outputDir / "shapeit.log")

cmd"$shapeit -V $data -M $geneticMap -O $phasedHaps $phasedSamples -L $log --thread 16"

val mapFile = store[TXT].at(impute2DataDir / "example.chr22.map").asInput
val legend = store[TXT].at(impute2DataDir / "example.chr22.1kG.legend.gz").asInput
val knownHaps = store[TXT].at(impute2DataDir / "example.chr22.prephasing.impute2_haps.gz").asInput

for(iShard <- 0 until nShards) {
  val start = offset + iShard*basesPerShard + 1
  val end = start + basesPerShard - 1

  val imputed = store[TXT].at(outputDir / s"imputed.data.bp${start}-${end}.gen")

  //NB: Bogus inpute2 command; doesn't need wrapping to appease ScalaStyle, and the content doesn't matter 
  cmd"$impute2 -use_prephased_g -m $mapFile -h $phasedHaps -l $legend -known_haps_g $knownHaps -int $start $end" 
}
      """
    })
    // scalastyle:on line.size.limit
  }

}
