package loamstream.loam

import java.nio.file.Path
import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.cli.Conf
import loamstream.conf.LsSettings
import loamstream.drm.DrmSystem
import loamstream.model.execute.Executable
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript.Implicits.BashPath
import loamstream.util.DirOracle
import loamstream.util.jvm.JvmArgs
import loamstream.util.jvm.SysPropNames
import loamstream.model.jobs.DataHandle
import scala.collection.compat._


/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
final class LoamToolBoxTest extends FunSuite {

  import loamstream.TestHelpers.path

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
    assert(jobNode.dependencies.size === 1)

    toClj(jobNode.dependencies.head)
  }

  private def hasNoDeps(jobNode: JobNode): Boolean = jobNode.dependencies.isEmpty

  import loamstream.util.Paths.Implicits._

  test("Simple toy pipeline using cp.") {

    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    val fileIn = outDir / "fileIn.txt"
    val fileOut1 = outDir / "fileOut1.txt"
    val fileOut2 = outDir / "fileOut2.txt"
    val fileOut3 = outDir / "fileOut3.txt"
    val fileOut4 = outDir / "fileOut4.txt"

    val (graph, inToOne, oneToTwo, twoToThree, twoToFour) = TestHelpers.withScriptContext { implicit context =>
      import loamstream.loam.LoamSyntax._

      val fileInStore = store(fileIn).asInput
      val fileOut1Store = store(fileOut1)
      val fileOut2Store = store(fileOut2)
      val fileOut3Store = store(fileOut3)
      val fileOut4Store = store(fileOut4)
      val inToOne = cmd"cp $fileInStore $fileOut1Store".in(fileInStore).out(fileOut1Store)
      val oneToTwo = cmd"cp $fileOut1Store $fileOut2Store".in(fileOut1Store).out(fileOut2Store)
      val twoToThree = cmd"cp $fileOut2Store $fileOut3Store".in(fileOut2Store).out(fileOut3Store)
      val twoToFour = cmd"cp $fileOut2Store $fileOut4Store".in(fileOut2Store).out(fileOut4Store)

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
    
    import DataHandle.PathHandle
    
    assert(inToOneJob.inputs === Set(PathHandle(fileIn)))
    assert(inToOneJob.outputs === Set(PathHandle(fileOut1)))
    
    assert(oneToTwoJob.inputs === Set(PathHandle(fileOut1)))
    assert(oneToTwoJob.outputs === Set(PathHandle(fileOut2)))
    
    assert(twoToThreeJob.inputs === Set(PathHandle(fileOut2)))
    assert(twoToThreeJob.outputs === Set(PathHandle(fileOut3)))
    
    assert(twoToFourJob.inputs === Set(PathHandle(fileOut2)))
    assert(twoToFourJob.outputs === Set(PathHandle(fileOut4)))
  }

  test("Simple toy pipeline using cp, some tools named; ensure tool names are propagated to jobs.") {
    val outDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    val fileInPath = outDir / "fileIn.txt"
    val fileOut1Path = outDir / "fileOut1.txt"
    val fileOut2Path = outDir / "fileOut2.txt"
    val fileOut3Path = outDir / "fileOut3.txt"

    val (graph, fileTmp1Path, fileTmp2Path, inToTmp1, tmp1ToTmp2, tmp2ToOne, tmp2ToTwo, tmp2ToThree) = {
      TestHelpers.withScriptContext { implicit context =>
        import loamstream.loam.LoamSyntax._

        val fileIn = store(fileInPath).asInput
        val fileTmp1 = store
        val fileTmp2 = store
        val fileOut1 = store(fileOut1Path)
        val fileOut2 = store(fileOut2Path)
        val fileOut3 = store(fileOut3Path)

        val inToTmp1 = cmd"cp $fileIn $fileTmp1"
        val tmp1ToTmp2 = cmd"cp $fileTmp1 $fileTmp2"
        val tmp2ToOne = cmd"cp $fileTmp2 $fileOut1"
        val tmp2ToTwo = cmd"cp $fileTmp2 $fileOut2".tag("2to2")
        val tmp2ToThree = cmd"cp $fileTmp2 $fileOut3".tag("2to3")

        val graph = context.projectContext.graph

        (graph, fileTmp1.render, fileTmp2.render, inToTmp1, tmp1ToTmp2, tmp2ToOne, tmp2ToTwo, tmp2ToThree)
      }
    }

    val executable = toJobs(graph)

    assert(executable.jobNodes.size === 3)

    assert(graph.nameOf(inToTmp1).isDefined)
    assert(graph.nameOf(tmp1ToTmp2).isDefined)
    assert(graph.nameOf(tmp2ToOne).isDefined)
    assert(graph.namedTools.getByValue("2to2") === Some(tmp2ToTwo))
    assert(graph.namedTools.getByValue("2to3") === Some(tmp2ToThree))

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

    assert(jobInToTmp1.name.startsWith(LoamGraph.autogeneratedToolNamePrefix))
    assert(jobTmp1ToTmp2.name.startsWith(LoamGraph.autogeneratedToolNamePrefix))
    assert(jobTmp2To1.name.startsWith(LoamGraph.autogeneratedToolNamePrefix))
    assert(jobTmp2To2.name === "2to2")
    assert(jobTmp2To3.name === "2to3")
  }

  test("""|'real' pipeline: 3 impute2 invocations that all depend on the same shapeit invocation
          |should produce expected job tree""".stripMargin) {

    val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)

    val softDir = path("/path/to/binaries")

    val shapeit = softDir / "shapeit"
    val impute2 = softDir / "impute2"

    val nShards = 3

    val (graph, shapeitTool, Seq(imputeTool0, imputeTool1, imputeTool2)) = {
      TestHelpers.withScriptContext { implicit context =>
        //Derived from a previous real-world .loam

        import loamstream.loam.LoamSyntax._

        val offset = 20400000
        val basesPerShard = 100000

        val dataDir = workDir / "imputation"
        val shapeitDataDir = dataDir / "shapeit_example"
        val impute2DataDir = dataDir / "impute2_example"

        val outputDir = workDir / "output"

        val input = store(shapeitDataDir / "gwas.vcf.gz").asInput
        val shapeitOuput = store(outputDir / "phased.samples.gz")
        val log = store(outputDir / "shapeit.log")

        val shapeitTool = cmd"$shapeit -i $input -o $shapeitOuput".in(input).out(shapeitOuput)

        val imputeTools = for(iShard <- 0 until nShards) yield {
          val start = offset + (iShard * basesPerShard) + 1
          val end = start + basesPerShard - 1

          val imputed = store(outputDir / s"imputed.data.bp${start}-${end}.gen")

          val imputeTool = cmd"$impute2 -i $shapeitOuput -o $imputed".in(shapeitOuput).out(imputed)

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
  
  private object DummyOracles {
    def alwaysReturns[A](workDir: Path): DirOracle[A] = new DirOracle[A] {
      override def dirOptFor(job: A): Option[Path] = Some(workDir)
    }
  
    def alwaysNone[A]: DirOracle[A] = new DirOracle[A] {
      override def dirOptFor(job: A): Option[Path] = None
    }
  }
  
  test("makeWorkerJobCommandLineTokens - cliConfig guard") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val (graph, tool) = TestHelpers.withScriptContext(LsSettings(cliConfig = None)) { implicit scriptContext =>
        val tool = InvokesLsTool().tag("foo")
      
        (scriptContext.projectContext.graph, tool)
      }

      //Should throw, no Conf.Values in LsSettings
      intercept[Exception] {
        LoamToolBox.makeWorkerJobCommandLineTokens(graph, tool, DummyOracles.alwaysReturns(workDir))
      }
    }
  }
  
  test("makeWorkerJobCommandLineTokens - workDir guard") {
    val cliConf = Conf("--backend uger --loams foo.loam".split("\\s+")).toValues
    
    val (graph, tool) = TestHelpers.withScriptContext(LsSettings(Some(cliConf))) { implicit scriptContext =>
      val tool = InvokesLsTool().tag("foo")
      
      (scriptContext.projectContext.graph, tool)
    }

    //Should throw, can't determine work dir for tool invocation
    intercept[Exception] {
      LoamToolBox.makeWorkerJobCommandLineTokens(graph, tool, DummyOracles.alwaysNone)
    }
  }
  
  test("makeWorkerJobCommandLineTokens - DRM system guard") {
    val cliConf = Conf("--backend uger --loams foo.loam".split("\\s+")).toValues
    
    val (graph, tool) = {
      implicit val scriptContext = {
        val withoutDrmSystem = TestHelpers.config.copy(drmSystem = None)
      
        new LoamScriptContext(LoamProjectContext.empty(withoutDrmSystem, LsSettings(Some(cliConf))))
      }
      
      val tool = InvokesLsTool().tag("foo")
      
      (scriptContext.projectContext.graph, tool)
    }

    //Should throw, no DRM system defined in LoamConfig
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      intercept[Exception] {
        LoamToolBox.makeWorkerJobCommandLineTokens(graph, tool, DummyOracles.alwaysReturns(workDir))
      }
    }
  }
  
  test("makeWorkerJobCommandLineTokens - user-specified-tag guard") {
    val cliConf = Conf("--backend uger --loams foo.loam".split("\\s+")).toValues
    
    val (graph, tool) = {
      implicit val scriptContext = {
        val withoutDrmSystem = TestHelpers.config.copy(drmSystem = Some(DrmSystem.Uger))
      
        new LoamScriptContext(LoamProjectContext.empty(withoutDrmSystem, LsSettings(Some(cliConf))))
      }
      
      val tool = InvokesLsTool() //Note no .tag(...)
      
      (scriptContext.projectContext.graph, tool)
    }

    //Should throw, no tag on InvokesLsTool
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      intercept[Exception] {
        LoamToolBox.makeWorkerJobCommandLineTokens(graph, tool, DummyOracles.alwaysReturns(workDir))
      }
    }
  }
    
  test("makeWorkerJobCommandLineTokens - happy path") {
    val args = "--backend uger --loams foo.loam".split("\\s+").to(List)
    
    val jobName = "some-job"
    
    val cliConf = Conf(args).toValues
    
    val jvmArgs = JvmArgs(Seq("jvmArg0", "jvmArg1", "jvmArg2"), "some-class-path")
    
    val (graph, tool) = {
      import LoamSyntax._
      
      implicit val scriptContext = {
        val withoutDrmSystem = TestHelpers.config.copy(drmSystem = Some(DrmSystem.Uger))
      
        new LoamScriptContext(LoamProjectContext.empty(withoutDrmSystem, LsSettings(cliConf, jvmArgs)))
      }
      
      val tool = InvokesLsTool().tag(jobName).using("Bar-2.0", "Baz-3.1")
      
      (scriptContext.projectContext.graph, tool)
    }

    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      def makePreambles(dotkits: String*): String = {
        val useuse = "source /broad/software/scripts/useuse"
        val and = "&&"
        val reuse = "reuse -q"
        val reuses = dotkits.mkString(s"$reuse ", s" $and $reuse ", s" $and")
      
        s"$useuse $and $reuses"
      }
      
      val expectedJavaBinary = Paths.get(System.getProperty("java.home")).resolve("bin").resolve("java")
      
      val sysprops = Map(
          SysPropNames.loamstreamWorkDir -> workDir.toString,
          SysPropNames.loamstreamExecutionLoamstreamDir -> workDir.toString)
      
      val expected = Seq(
        makePreambles("Bar-2.0", "Baz-3.1"),
        expectedJavaBinary.toString,
        "jvmArg0", 
        "jvmArg1", 
        "jvmArg2",
        s"-D${SysPropNames.loamstreamWorkDir}=${workDir}",
        s"-D${SysPropNames.loamstreamExecutionLoamstreamDir}=${workDir}",
        "-jar",
        "some-class-path",
        "--worker",
        "--backend", "uger",
        "--run", "allOf", jobName,
        "--loams", "foo.loam")
      
      val actual = LoamToolBox.makeWorkerJobCommandLineTokens(graph, tool, DummyOracles.alwaysReturns(workDir))
        
      assert(actual === expected)
    }
  }
}

