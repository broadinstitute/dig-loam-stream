package loamstream.loam

import java.nio.file.{Paths, Files => JFiles}

import loamstream.LEnv
import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.loam.LoamToolBoxTest.Results
import loamstream.loam.ast.{LoamGraphAstMapper, LoamGraphAstMapping}
import loamstream.model.execute.ChunkedExecuter
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob.CommandSuccess
import loamstream.util.{Files, Hit, Shot}
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global
import java.nio.file.Path
import scala.util.Try
import loamstream.LoamTestHelpers

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBoxTest extends FunSuite with LoamTestHelpers {
  
  private def run(code: String): Results = {
    val compileResults = compile(code)
    
    val (mapping, executable) = toExecutable(compileResults)
    
    val jobResults = run(executable)
    
    val env = compileResults.envOpt.get

    val graph = compileResults.graphOpt.get.withEnv(env)
    
    Results(env, graph, mapping, jobResults)
  }

  test("Simple toy pipeline using cp.") {
    //Make files in target/ so we don't risk cluttering up the project root directory if anything goes wrong.
    withFiles("target/fileIn.txt", "target/fileOut1.txt", "target/fileOut2.txt", "target/fileOut3.txt") { paths =>
      val Seq(fileIn, fileOut1, fileOut2, fileOut3) = paths
    
      Files.writeTo(fileIn)("Hello World!")
      
      val code = LoamToolBoxTest.Sources.toyCp

      val results = run(code)
      
      assert(results.allJobResultsAreSuccess)
      assert(results.jobResults.size === 5)
      assert(results.mapping.rootAsts.size === 3)
      assert(results.mapping.rootTools.size === 3)
      
      assert(JFiles.exists(fileIn))
      assert(JFiles.exists(fileOut1))
      assert(JFiles.exists(fileOut2))
      assert(JFiles.exists(fileOut3))
    }
  }
  
  test("'real' pipeline: 3 impute2 invocations that all depend on the same shapeit invocation should produce expected ASTs and job graph") {
    val source = LoamToolBoxTest.Sources.imputeParallel
    
    val compileResult = compile(source)
    
    if(!compileResult.isValid) {
      fail(s"Could not compile '$source': ${compileResult.errors}")
    }
    
    val (mapping, executable) = toExecutable(compileResult)
    
    assert(mapping.toolAsts.size == 4)
    assert(mapping.rootAsts.size == 3)
    
    def getRootAst(i: Int) = mapping.rootAsts.toSeq.apply(i)
    
    assert(getRootAst(0).dependencies.size == 1)
    assert(getRootAst(1).dependencies.size == 1)
    assert(getRootAst(2).dependencies.size == 1)
    
    val dep0 = getRootAst(0).dependencies.head.producer
    val dep1 = getRootAst(1).dependencies.head.producer
    val dep2 = getRootAst(2).dependencies.head.producer
    
    assert(dep0 eq dep1)
    assert(dep1 eq dep2)
    assert(dep0 eq dep2)
    
    assert(executable.jobs.size == 3)
    
    def getJob(i: Int) = executable.jobs.toSeq.apply(i)
    
    assert(getJob(0).inputs.size == 1)
    assert(getJob(1).inputs.size == 1)
    assert(getJob(2).inputs.size == 1)
    
    val depJob0 = getJob(0).inputs.head
    val depJob1 = getJob(1).inputs.head
    val depJob2 = getJob(2).inputs.head
    
    assert(depJob0 eq depJob1)
    assert(depJob1 eq depJob2)
    assert(depJob0 eq depJob2)
  }
  
  private def withFiles[A](names: String*)(f: Seq[Path] => A): A = {
    //TODO: use Commons-IO, like elsewhere 
    def deleteQuietly(p: Path): Unit = Try(JFiles.delete(p))
      
    val paths = names.map(Paths.get(_))
    
    paths.foreach(deleteQuietly)
    
    try { f(paths) }
    finally { paths.foreach(deleteQuietly) }
  }
}

object LoamToolBoxTest {

  final case class Results(env: LEnv, graph: LoamGraph, mapping: LoamGraphAstMapping,
                        jobResults: Map[LJob, Shot[LJob.Result]]) {
    def allJobResultsAreSuccess: Boolean = jobResults.values.forall {
      case Hit(CommandSuccess(_, _)) => true
      case _ => false
    }
  }
  
  object Sources {
    val toyCp = {
      """
        |val fileIn = store[String].from(path("target/fileIn.txt"))
        |val fileTmp1 = store[String]
        |val fileTmp2 = store[String]
        |val fileOut1 = store[String].to(path("target/fileOut1.txt"))
        |val fileOut2 = store[String].to(path("target/fileOut2.txt"))
        |val fileOut3 = store[String].to(path("target/fileOut3.txt"))
        |cmd"cp $fileIn $fileTmp1"
        |cmd"cp $fileTmp1 $fileTmp2"
        |cmd"cp $fileTmp2 $fileOut1"
        |cmd"cp $fileTmp2 $fileOut2"
        |cmd"cp $fileTmp2 $fileOut3"
      """.stripMargin
    }
    
    val imputeParallel = {
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

val data = store[VCF].from(shapeitDataDir / "gwas.vcf.gz")
val geneticMap = store[TXT].from(shapeitDataDir / "genetic_map.txt.gz")
val phasedHaps = store[TXT].to(outputDir / "phased.haps.gz")
val phasedSamples = store[TXT].to(outputDir / "phased.samples.gz")
val log = store[TXT].to(outputDir / "shapeit.log")

cmd"$shapeit -V $data -M $geneticMap -O $phasedHaps $phasedSamples -L $log --thread 16"

val mapFile = store[TXT].from(impute2DataDir / "example.chr22.map")
val legend = store[TXT].from(impute2DataDir / "example.chr22.1kG.legend.gz")
val knownHaps = store[TXT].from(impute2DataDir / "example.chr22.prephasing.impute2_haps.gz")

for(iShard <- 0 until nShards) {
  val start = offset + iShard*basesPerShard + 1
  val end = start + basesPerShard - 1

  val imputed = store[TXT].to(outputDir / s"imputed.data.bp${start}-${end}.gen")

  cmd"$impute2 -use_prephased_g -m $mapFile -h $phasedHaps -l $legend -known_haps_g $knownHaps -int $start $end -Ne 20000 -o $imputed -verbose -o_gz"
}
"""
    }
  }
}