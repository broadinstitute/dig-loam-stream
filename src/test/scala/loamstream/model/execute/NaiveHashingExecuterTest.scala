package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.LJob
import loamstream.db.slick.SlickLoamDao
import loamstream.db.slick.DbDescriptor
import java.io.File
import java.nio.file.Paths
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.db.slick.AbstractSlickLoamDaoTest
import loamstream.db.TestDbDescriptors
import java.nio.file.Path
import loamstream.util.Sequence
import loamstream.util.PathEnrichments
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.Output
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.commandline.CommandLineJob.CommandResult
import loamstream.model.jobs.JobState
import loamstream.util.Hashes
import loamstream.model.jobs.commandline.CommandLineStringJob

/**
 * @author clint
 *         date: Jun 2, 2016
 * @author kyuksel
 *         date: Jul 15, 2016
 */
final class NaiveHashingExecuterTest extends FunSuite with AbstractSlickLoamDaoTest {

  override val descriptor = TestDbDescriptors.inMemoryH2
  
  private def executer = new NaiveHashingExecuter(dao)(ExecutionContext.global)

  private def toyCpPipeline(workDir: Path) = {
    """val workDir = path("""" + workDir + """")
      |val fileIn = store[String].from(path("src/test/resources/a.txt"))
      |val fileOut1 = store[String].to(workDir / "fileOut1.txt")
      |val fileOut2 = store[String].to(workDir / "fileOut2.txt")
      |val fileOut3 = store[String].to(workDir / "fileOut3.txt")
      |cmd"cp $fileIn $fileOut1".out(fileOut1)
      |cmd"cp $fileOut1 $fileOut2".out(fileOut2)
      |cmd"cp $fileOut2 $fileOut3".out(fileOut3)
    """.stripMargin.trim
  }

  test("Pipelines can be resumed after stopping 1/3rd of the way through") {
    val workDir = makeWorkDir()
    
    import java.nio.file.{ Files => JFiles }
    
    def path(s: String) = Paths.get(s)
    
    import PathEnrichments._
    
    val start = path("src/test/resources/a.txt")
    val f1 = workDir / "fileOut1.txt"
    val f2 = workDir / "fileOut2.txt"
    val f3 = workDir / "fileOut3.txt"
    
    val cwd = Paths.get(".")
    
    val startToF1 = CommandLineStringJob(s"cp $start $f1", cwd, outputs = Set(Output.PathOutput(f1))) 
    val f1ToF2 = CommandLineStringJob(s"cp $f1 $f2", cwd, outputs = Set(Output.PathOutput(f2)), inputs = Set(startToF1))
    val f2ToF3 = CommandLineStringJob(s"cp $f2 $f3", cwd, outputs = Set(Output.PathOutput(f3)), inputs = Set(f1ToF2))
    
    assert(startToF1.state == JobState.NotStarted)
    assert(f1ToF2.state == JobState.NotStarted)
    assert(f2ToF3.state == JobState.NotStarted)
    
    val executable = LExecutable(Set(f2ToF3))
    
    createTablesAndThen {
      assert(f1.toFile.exists == false)
      
      JFiles.copy(start, f1)
      
      dao.storeHash(f1, Hashes.sha1(f1))
      
      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))
      
      val jobResults = executer.execute(executable)
    
      assert(startToF1.state == JobState.NotStarted)
      assert(f1ToF2.state == JobState.Finished)
      assert(f2ToF3.state == JobState.Finished)
      
      assert(jobResults.size == 2)
      
      //jobResults.filter { case (j, r) => r.isMiss || r.get.isFailure }.foreach(println)
      
      assert(jobResults.values.forall(_.get.isSuccess))
    }
  }
  
  test("Pipelines can be resumed after stopping 2/3rds of the way through") {
    val workDir = makeWorkDir()
    
    import java.nio.file.{ Files => JFiles }
    
    def path(s: String) = Paths.get(s)
    
    import PathEnrichments._
    
    val start = path("src/test/resources/a.txt")
    val f1 = workDir / "fileOut1.txt"
    val f2 = workDir / "fileOut2.txt"
    val f3 = workDir / "fileOut3.txt"
    
    val cwd = Paths.get(".")
    
    val startToF1 = CommandLineStringJob(s"cp $start $f1", cwd, outputs = Set(Output.PathOutput(f1))) 
    val f1ToF2 = CommandLineStringJob(s"cp $f1 $f2", cwd, outputs = Set(Output.PathOutput(f2)), inputs = Set(startToF1))
    val f2ToF3 = CommandLineStringJob(s"cp $f2 $f3", cwd, outputs = Set(Output.PathOutput(f3)), inputs = Set(f1ToF2))
    
    assert(startToF1.state == JobState.NotStarted)
    assert(f1ToF2.state == JobState.NotStarted)
    assert(f2ToF3.state == JobState.NotStarted)
    
    val executable = LExecutable(Set(f2ToF3))
    
    createTablesAndThen {
      assert(f1.toFile.exists == false)
      
      JFiles.copy(start, f1)
      JFiles.copy(start, f2)
      
      dao.storeHash(f1, Hashes.sha1(f1))
      dao.storeHash(f2, Hashes.sha1(f2))
      
      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))
      
      val jobResults = executer.execute(executable)
    
      assert(startToF1.state == JobState.NotStarted)
      assert(f1ToF2.state == JobState.NotStarted)
      assert(f2ToF3.state == JobState.Finished)
      
      assert(jobResults.size == 1)
      
      //jobResults.filter { case (j, r) => r.isMiss || r.get.isFailure }.foreach(println)
      
      assert(jobResults.values.forall(_.get.isSuccess))
    }
  }
  
  test("Pipelines can be run from scratch") {
    
    val workDir = makeWorkDir()
    
    val executable = compile(toyCpPipeline(workDir))
    
    createTablesAndThen {
      val jobResults = executer.execute(executable)
    
      assert(jobResults.size == 4)
    
      //jobResults.filter { case (j, r) => r.isMiss || r.get.isFailure }.foreach(println)
    
      assert(jobResults.values.forall(_.get.isSuccess))
    }
  }
    
  private lazy val compiler = new LoamCompiler(OutMessageSink.NoOp)
  
  private def compile(loamCode: String): LExecutable = {
    
    val compileResults = compiler.compile(loamCode)

    assert(compileResults.errors == Nil)
    
    val graph = compileResults.graphOpt.get

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = new LoamToolBox

    mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _).plusNoOpRootJob
  }
  
  private def executionCount(job: LJob): Int = job.asInstanceOf[MockJob].executionCount

  private val sequence: Sequence[Int] = Sequence()
  
  private def makeWorkDir(): Path = {
    def exists(path: Path): Boolean = path.toFile.exists
    
    val suffixes = Iterator.continually(sequence.next())
    
    val candidates = suffixes.map(i => Paths.get(s"target/hashing-executer-test$i"))
    
    val result = candidates.dropWhile(exists).next()
    
    val asFile = result.toFile
    
    asFile.mkdir()

    assert(asFile.exists)
    
    result
  }
}
