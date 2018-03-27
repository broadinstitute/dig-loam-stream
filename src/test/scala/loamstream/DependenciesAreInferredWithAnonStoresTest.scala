package loamstream

import org.scalatest.FunSuite
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamCmdTool
import loamstream.util.PathEnrichments
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.LJob
import loamstream.util.Files
import java.io.IOException

/**
 * @author clint
 * Mar 26, 2018
 */
final class DependenciesAreInferredWithAnonStoresTest extends FunSuite {
  import TestHelpers.path
  import java.nio.file.Files.exists
  import PathEnrichments._
  
  test("Copying a nonexistent file to an anonymous store should fail as expected") {
    val nonExistentInputPath = path("/some/nonexistent/path/in")
    
    assert(exists(nonExistentInputPath) === false)
    
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val nonExistent = store.at(nonExistentInputPath).asInput
      
      val anonOutput = store //Note anonymous store
      
      cmd"cp $nonExistent $anonOutput".in(nonExistent).out(anonOutput)
    }
    
    val resultMap = TestHelpers.loamEngine.run(graph)
    
    assert(resultMap.values.head.status.isFailure)
    
    assert(resultMap.size === 1)
  }
  
  test("Copying a file that exists to an anonymous store should work") {
    val inputPath = path("src/test/resources/a.txt")
    
    assert(exists(inputPath))
    
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val (graph, anonOutput) = TestHelpers.withScriptContext { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val in = store.at(inputPath).asInput
      
      val anonOutput = store //Note anonymous store
      
      cmd"cp $in $anonOutput".in(in).out(anonOutput)
      
      (context.projectContext.graph, anonOutput)
    }
    
    val resultMap = TestHelpers.loamEngine.run(graph)
    
    assert(resultMap.values.head.isSuccess)
    
    assert(resultMap.size === 1)
    
    assert(exists(anonOutput.path))
    
    assert(Files.readFrom(inputPath) === Files.readFrom(anonOutput.path))
  }
  
  test("A pipeline that copies a nonexistent file via an intermediate anonymous store should fail as expected") {
    val nonExistentInputPath = path("/some/nonexistent/path/in")
    
    assert(exists(nonExistentInputPath) === false)
    
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val outPath = workDir / "out"
    
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val nonExistent = store.at(nonExistentInputPath).asInput
      
      val intermediate = store //Note anonymous store
      
      val out = store.at(outPath)
      
      cmd"cp $nonExistent $intermediate".in(nonExistent).out(intermediate)
      
      cmd"cp $intermediate $out".in(intermediate).out(out)
    }
    
    assert(exists(outPath) === false)
    
    val resultMap = TestHelpers.loamEngine.run(graph)
    
    assert(exists(outPath) === false)
    
    val firstJob: LJob = {
      resultMap.keys.find(_.asInstanceOf[CommandLineJob].commandLineString.contains("nonexistent")).get
    }
    
    assert(resultMap(firstJob).status.isFailure)
    
    assert(resultMap.size === 1)
  }
  
  test("A pipeline that copies an existing file via an intermediate anonymous store should work") {
    val inputPath = path("src/test/resources/a.txt")
    
    assert(exists(inputPath))
    
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val outPath = workDir / "out"
    
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val in = store.at(inputPath).asInput
      
      val intermediate = store //Note anonymous store
      
      val out = store.at(outPath)
      
      cmd"cp $in $intermediate".in(in).out(intermediate)
      
      cmd"cp $intermediate $out".in(intermediate).out(out)
    }
    
    assert(exists(outPath) === false)
    
    val resultMap = TestHelpers.loamEngine.run(graph)
    
    assert(exists(outPath))
    
    val secondJob: LJob = resultMap.keys.find(_.asInstanceOf[CommandLineJob].commandLineString.endsWith("out")).get
    val firstJob: LJob = resultMap.keys.find(_ != secondJob).get
    
    assert(resultMap(firstJob).status.isSuccess)
    assert(resultMap(secondJob).status.isSuccess)
    
    assert(resultMap.size === 2)

    assert(exists(outPath))
    
    assert(Files.readFrom(inputPath) === Files.readFrom(outPath))
  }
  
  test("A pipeline that copies an nonexistant file via an intermediate anonymous store to 2 outputs should work") {
    val nonExistentInputPath = path("/some/nonexistent/path/in")
    
    assert(exists(nonExistentInputPath) === false)
    
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val outPath0 = workDir / "out0"
    val outPath1 = workDir / "out1"
    
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val nonExistent = store.at(nonExistentInputPath).asInput
      
      val intermediate = store //Note anonymous store
      
      val out0 = store.at(outPath0)
      val out1 = store.at(outPath1)
      
      cmd"cp $nonExistent $intermediate".in(nonExistent).out(intermediate)
      
      cmd"cp $intermediate $out0".in(intermediate).out(out0)
      cmd"cp $intermediate $out1".in(intermediate).out(out1)
    }
    
    assert(exists(outPath0) === false)
    assert(exists(outPath1) === false)
    
    val resultMap = TestHelpers.loamEngine.run(graph)
    
    assert(exists(outPath0) === false)
    assert(exists(outPath1) === false)
    
    def findJob(predicate: String => Boolean): LJob = {
      resultMap.keys.find(job => predicate(job.asInstanceOf[CommandLineJob].commandLineString.trim)).get
    }
    
    val firstJob: LJob = findJob(_.contains("nonexistent"))
    
    assert(resultMap(firstJob).status.isFailure)
    
    assert(resultMap.size === 1)

    assert(exists(outPath0) === false)
    assert(exists(outPath1) === false)
  }
  
  ignore("src/examples/loam/cp.loam") {
    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val nonExistentInputPath = path("/some/nonexistent/path/in")
    
    val (graph, anon0, anon1) = TestHelpers.withScriptContext { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val nonexistent = store.at(nonExistentInputPath).asInput
      val fileTmp1 = store
      val fileTmp2 = store
      val fileOut1 = store.at(workDir / "fileOut1.txt")
      val fileOut2 = store.at(workDir / "fileOut2.txt")
      val fileOut3 = store.at(workDir / "fileOut3.txt")
      cmd"cp $nonexistent $fileTmp1".in(nonexistent).out(fileTmp1)
      cmd"cp $fileTmp1 $fileTmp2".in(fileTmp1).out(fileTmp2)
      cmd"cp $fileTmp2 $fileOut1".in(fileTmp2).out(fileOut1)
      cmd"cp $fileTmp2 $fileOut2".in(fileTmp2).out(fileOut2)
      cmd"cp $fileTmp2 $fileOut3".in(fileTmp2).out(fileOut3)
      
      (context.projectContext.graph, fileTmp1, fileTmp2)
    }
    
    assert(exists(nonExistentInputPath) === false)
    
    val resultMap = TestHelpers.loamEngine.run(graph)

    for {
      (job, execution) <- resultMap
    } {
      println(s"'${job.asInstanceOf[CommandLineJob].commandLineString}' => ${execution.result.get} (${Files.readFrom(execution.outputStreams.get.stderr).trim})")
    }
    
    println(s"Anon0 path: ${anon0.path}")
    println(s"Anon1 path: ${anon1.path}")
    
    assert(exists(anon0.path) === false)
    assert(exists(anon1.path) === false)
    
    //assert(resultMap.values.forall(_.isFailure))
    
    assert(resultMap.size === 1)
  }
}
