package loamstream.loam

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamPredef
import loamstream.model.execute.Executable
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript.Implicits._
import loamstream.util.PathEnrichments


/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBoxTest extends FunSuite {

  private def toJobs(graph: LoamGraph): Executable = {
    val toolBox = new LoamToolBox()

    toolBox.createExecutable(graph)
  }

  private def toClj(jn: JobNode) = jn.job.asInstanceOf[CommandLineJob]

  private def toCommandLine(jn: JobNode) = toClj(jn).commandLineString

  private def findJobWith(allJobs: Iterable[JobNode])(cl: String): CommandLineJob = {
    allJobs.iterator.map(toClj).find(_.commandLineString == cl).get
  }

  private def findJobBasedOn(allJobs: Iterable[JobNode])(tool: LoamCmdTool): CommandLineJob = {
    findJobWith(allJobs)(tool.commandLine)
  }

  private def singleDepOf(jobNode: JobNode): CommandLineJob = {
    assert(jobNode.inputs.size === 1)

    toClj(jobNode.inputs.head)
  }

  private def hasNoDeps(jobNode: JobNode): Boolean = jobNode.inputs.isEmpty

  import PathEnrichments._
  
  test("Simple one-command pipeline") {
    val (graph, tool) = TestHelpers.withScriptContext { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val tool = cmd"echo 'hello world!'"()
      
      (context.projectContext.graph, tool)
    }

    assert(graph.tools === Set(tool))
    assert(graph.finalTools === Set(tool)) 
    
    val executable = toJobs(graph)
    
    assert(executable.jobNodes.size === 1)
    
    assert(executable.jobNodes.head.job.asInstanceOf[CommandLineJob].commandLineString === tool.commandLine)
  }
  
  test("Simple two-command pipeline") {
    val (graph, tool0, tool1) = TestHelpers.withScriptContext { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
      
      val input = store.at("foo.txt").asInput
      val out0 = store.at("bar.txt")
      val out1 = store.at("baz.txt")
      
      val tool0 = cmd"cp $input $out0"(in = Seq(input), out = Seq(out0))
      
      val tool1 = cmd"cp $out0 $out1"(in = Seq(out0), out = Seq(out1))
      
      (context.projectContext.graph, tool0, tool1)
    }

    assert(graph.tools === Set(tool0, tool1))
    
    //assert(graph.finalTools === Set(tool1))
    
    val executable = toJobs(graph)
    
    assert(executable.jobNodes.size === 1)
    
    val rootJob = executable.jobNodes.head.job.asInstanceOf[CommandLineJob]
    
    assert(rootJob.commandLineString === tool1.commandLine)
    
    assert(rootJob.inputs.size === 1)
    
    assert(rootJob.inputs.head.job.asInstanceOf[CommandLineJob].commandLineString === tool0.commandLine)
  }

  test("Simple toy pipeline using cp.") {

    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    val fileIn = outDir / "fileIn.txt"
    val fileOut1 = outDir / "fileOut1.txt"
    val fileOut2 = outDir / "fileOut2.txt"
    val fileOut3 = outDir / "fileOut3.txt"
    val fileOut4 = outDir / "fileOut4.txt"

    val (graph, inToOne, oneToTwo, twoToThree, twoToFour) = TestHelpers.withScriptContext { implicit context =>
      import LoamPredef._
      import LoamCmdTool._

      val fileInStore = store.at(fileIn).asInput
      val fileOut1Store = store.at(fileOut1)
      val fileOut2Store = store.at(fileOut2)
      val fileOut3Store = store.at(fileOut3)
      val fileOut4Store = store.at(fileOut4)
      val inToOne = cmd"cp $fileInStore $fileOut1Store"(in = Seq(fileInStore), out = Seq(fileOut1Store))
      val oneToTwo = cmd"cp $fileOut1Store $fileOut2Store"(in = Seq(fileOut1Store), out = Seq(fileOut2Store))
      val twoToThree = cmd"cp $fileOut2Store $fileOut3Store"(in = Seq(fileOut2Store), out = Seq(fileOut3Store))
      val twoToFour = cmd"cp $fileOut2Store $fileOut4Store"(in = Seq(fileOut2Store), out = Seq(fileOut4Store))

      (context.projectContext.graph, inToOne, oneToTwo, twoToThree, twoToFour)
    }

    val executable = toJobs(graph)

    assert(executable.jobNodes.size === 2)

    val rootJobs = executable.jobNodes

    val allJobs = ExecuterHelpers.flattenTree(rootJobs)

    def findJob(t: LoamCmdTool): CommandLineJob = findJobBasedOn(allJobs)(t)

    assert(rootJobs.exists(jn => toCommandLine(jn) === twoToThree.commandLine))
    assert(rootJobs.exists(jn => toCommandLine(jn) === twoToFour.commandLine))

    val twoToThreeJob = findJob(twoToThree)
    val twoToFourJob = findJob(twoToFour)

    val oneToTwoJob = findJob(oneToTwo)

    assert(oneToTwoJob eq singleDepOf(twoToFourJob))
    assert(oneToTwoJob eq singleDepOf(twoToThreeJob))

    val inToOneJob = findJob(inToOne)

    assert(singleDepOf(oneToTwoJob) === inToOneJob)

    assert(hasNoDeps(inToOneJob))
  }

  test("Simple toy pipeline using cp, some tools named; ensure tool names are propagated to jobs.") {
    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    val fileInPath = outDir / "fileIn.txt"
    val fileOut1Path = outDir / "fileOut1.txt"
    val fileOut2Path = outDir / "fileOut2.txt"
    val fileOut3Path = outDir / "fileOut3.txt"

    val (graph, fileTmp1Path, fileTmp2Path, inToTmp1, tmp1ToTmp2, tmp2ToOne, tmp2ToTwo, tmp2ToThree) = {
      TestHelpers.withScriptContext { implicit context =>
        import LoamPredef._
        import LoamCmdTool._

        val fileIn = store.at(fileInPath).asInput
        val fileTmp1 = store
        val fileTmp2 = store
        val fileOut1 = store.at(fileOut1Path)
        val fileOut2 = store.at(fileOut2Path)
        val fileOut3 = store.at(fileOut3Path)

        val inToTmp1 = cmd"cp $fileIn $fileTmp1"()
        val tmp1ToTmp2 = cmd"cp $fileTmp1 $fileTmp2"()
        val tmp2ToOne = cmd"cp $fileTmp2 $fileOut1"()
        val tmp2ToTwo = cmd"cp $fileTmp2 $fileOut2"(name = "2to2")
        val tmp2ToThree = cmd"cp $fileTmp2 $fileOut3"(name = "2to3")

        val graph = context.projectContext.graph

        (graph, fileTmp1.render, fileTmp2.render, inToTmp1, tmp1ToTmp2, tmp2ToOne, tmp2ToTwo, tmp2ToThree)
      }
    }

    val executable = toJobs(graph)

    assert(executable.jobNodes.size === 3)

    assert(graph.namedTools("2to2") === tmp2ToTwo)
    assert(graph.namedTools("2to3") === tmp2ToThree)
    
    val toolsWithNames = graph.namedTools.values.toSet
    
    assert(toolsWithNames.contains(inToTmp1))
    assert(toolsWithNames.contains(tmp1ToTmp2))
    assert(toolsWithNames.contains(tmp2ToOne))
    
    assert(graph.namedTools.size === 5)

    val allJobs = ExecuterHelpers.flattenTree(executable.jobNodes).map(_.job)

    def findJob(t: LoamCmdTool): CommandLineJob = findJobBasedOn(allJobs)(t)

    def jobWithName(name: String): CommandLineJob = allJobs.iterator.map(toClj).find(_.name == name).get

    val jobTmp2To1 = findJob(tmp2ToOne)
    val jobTmp2To2 = jobWithName("2to2")
    val jobTmp2To3 = jobWithName("2to3")

    val jobTmp1ToTmp2 = findJob(tmp1ToTmp2)

    val jobInToTmp1 = findJob(inToTmp1)

    assert(singleDepOf(jobTmp2To1) eq jobTmp1ToTmp2)
    assert(singleDepOf(jobTmp2To2) eq jobTmp1ToTmp2)
    assert(singleDepOf(jobTmp2To2) eq jobTmp1ToTmp2)

    assert(singleDepOf(jobTmp1ToTmp2) eq jobInToTmp1)

    assert(hasNoDeps(jobInToTmp1))

    assert(jobTmp2To1.commandLineString === s"cp ${fileTmp2Path} ${fileOut1Path.render}")
    assert(jobTmp2To2.commandLineString === s"cp ${fileTmp2Path} ${fileOut2Path.render}")
    assert(jobTmp2To3.commandLineString === s"cp ${fileTmp2Path} ${fileOut3Path.render}")

    assert(jobTmp1ToTmp2.commandLineString === s"cp ${fileTmp1Path} ${fileTmp2Path}")

    assert(jobInToTmp1.commandLineString === s"cp ${fileInPath.render} ${fileTmp1Path}")

    val namedJobs: Set[LJob] = Set(jobTmp2To2, jobTmp2To3)

    val unnamedJobs = allJobs.filterNot(namedJobs)

    assert(unnamedJobs.forall(_.name !== "2to2"))
    assert(unnamedJobs.forall(_.name !== "2to3"))
  }

  test("""|'real' pipeline: 3 impute2 invocations that all depend on the same shapeit invocation
          |should produce expected job tree""".stripMargin) {

    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    val softDir = TestHelpers.path("/path/to/binaries")

    val shapeit = softDir / "shapeit"
    val impute2 = softDir / "impute2"

    val nShards = 3

    val (graph, shapeitTool, Seq(imputeTool0, imputeTool1, imputeTool2)) = {
      TestHelpers.withScriptContext { implicit context =>
        //Derived from a previous real-world .loam

        import LoamPredef._
        import LoamCmdTool._

        val offset = 20400000
        val basesPerShard = 100000

        val dataDir = workDir / "imputation"
        val shapeitDataDir = dataDir / "shapeit_example"
        val impute2DataDir = dataDir / "impute2_example"

        val outputDir = workDir / "output"

        val input = store.at(shapeitDataDir / "gwas.vcf.gz").asInput
        val shapeitOuput = store.at(outputDir / "phased.samples.gz")
        val log = store.at(outputDir / "shapeit.log")

        val shapeitTool = cmd"$shapeit -i $input -o $shapeitOuput"(in = Seq(input), out = Seq(shapeitOuput))

        val imputeTools = for(iShard <- 0 until nShards) yield {
          val start = offset + (iShard * basesPerShard) + 1
          val end = start + basesPerShard - 1

          val imputed = store.at(outputDir / s"imputed.data.bp${start}-${end}.gen")

          val imputeTool = cmd"$impute2 -i $shapeitOuput -o $imputed"(in = Seq(shapeitOuput), out = Seq(imputed))

          imputeTool
        }

        val graph = context.projectContext.graph

        (graph, shapeitTool, imputeTools)
      }
    }

    val executable = toJobs(graph)

    val allJobs = ExecuterHelpers.flattenTree(executable.jobNodes).map(_.job)

    assert(allJobs.size === 4)
    assert(executable.jobNodes.size === 3)

    def findJob(t: LoamCmdTool): CommandLineJob = findJobBasedOn(allJobs)(t)

    val shapeitJob = findJob(shapeitTool)

    assert(hasNoDeps(shapeitJob))

    val imputeJob0 = findJob(imputeTool0)
    val imputeJob1 = findJob(imputeTool1)
    val imputeJob2 = findJob(imputeTool2)

    assert(singleDepOf(imputeJob0) eq shapeitJob)
    assert(singleDepOf(imputeJob1) eq shapeitJob)
    assert(singleDepOf(imputeJob2) eq shapeitJob)

    assert(imputeJob0 ne imputeJob1)
    assert(imputeJob1 ne imputeJob2)
    assert(imputeJob2 ne imputeJob0)
  }
}

