package loamstream.model.execute

import java.nio.file.Path
import java.nio.file.Paths

import scala.concurrent.ExecutionContext

import org.scalatest.FunSuite

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.db.TestDbDescriptors
import loamstream.db.slick.AbstractSlickLoamDaoTest
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.util.Hashes
import loamstream.util.PathEnrichments
import loamstream.util.Sequence
import loamstream.model.jobs.MockJob
import scala.concurrent.Future

/**
 * @author clint
 * date: Aug 12, 2016
 */
final class NaiveHashingExecuterTest extends FunSuite with AbstractSlickLoamDaoTest {

  override val descriptor = TestDbDescriptors.inMemoryH2

  private def executer = new NaiveHashingExecuter(dao)(ExecutionContext.global)

  test("Pipelines can be resumed after stopping 1/3rd of the way through") {
    import JobState._

    doTest(Seq(NotStarted, Succeeded, Succeeded)) { (start, f1, f2, f3) =>
      import java.nio.file.{ Files => JFiles }

      JFiles.copy(start, f1)

      dao.storeHash(f1, Hashes.sha1(f1))

      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))
    }
  }

  test("Pipelines can be resumed after stopping 2/3rds of the way through") {

    import JobState._

    doTest(Seq(NotStarted, NotStarted, Succeeded)) { (start, f1, f2, f3) =>
      import java.nio.file.{ Files => JFiles }

      JFiles.copy(start, f1)
      JFiles.copy(start, f2)

      dao.storeHash(f1, Hashes.sha1(f1))
      dao.storeHash(f2, Hashes.sha1(f2))

      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))

      assert(f2.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f2))
    }
  }

  test("Re-running a finished pipelines does nothing") {

    import JobState._

    doTest(Seq(NotStarted, NotStarted, NotStarted)) { (start, f1, f2, f3) =>
      import java.nio.file.{ Files => JFiles }

      JFiles.copy(start, f1)
      JFiles.copy(start, f2)
      JFiles.copy(start, f3)

      dao.storeHash(f1, Hashes.sha1(f1))
      dao.storeHash(f2, Hashes.sha1(f2))
      dao.storeHash(f3, Hashes.sha1(f3))

      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))

      assert(f2.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f2))

      assert(f3.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f3))
    }
  }

  test("Every job is run for Pipelines with no existing outputs") {
    import JobState._

    doTest(Seq(Succeeded, Succeeded, Succeeded)) { (start, f1, f2, f3) =>
      //No setup
    }
  }

  private def mockJob(name: String, outputs: Set[Output], inputs: Set[LJob] = Set.empty)(body: => Any): MockJob = {
    def success(s: String) = LJob.SimpleSuccess(s)

    new MockJob(success(name), name, inputs, outputs, delay = 0) {
      override protected def executeSelf(implicit context: ExecutionContext): Future[LJob.Result] = {
        body

        super.executeSelf
      }
    }
  }

  private def doTest(expectations: Seq[JobState])(setup: (Path, Path, Path, Path) => Any): Unit = {
    import PathEnrichments._
    val workDir = makeWorkDir()

    def path(s: String) = Paths.get(s)

    val start = path("src/test/resources/a.txt")
    val f1 = workDir / "fileOut1.txt"
    val f2 = workDir / "fileOut2.txt"
    val f3 = workDir / "fileOut3.txt"

    import java.nio.file.{ Files => JFiles }

    val startToF1 = mockJob(s"cp $start $f1", Set(Output.PathOutput(f1))) {
      JFiles.copy(start, f1)
    }

    val f1ToF2 = mockJob(s"cp $f1 $f2", Set(Output.PathOutput(f2)), Set(startToF1)) {
      JFiles.copy(f1, f2)
    }

    val f2ToF3 = mockJob(s"cp $f2 $f3", Set(Output.PathOutput(f3)), Set(f1ToF2)) {
      JFiles.copy(f2, f3)
    }

    assert(startToF1.state == JobState.NotStarted)
    assert(f1ToF2.state == JobState.NotStarted)
    assert(f2ToF3.state == JobState.NotStarted)

    val executable = LExecutable(Set(f2ToF3))

    createTablesAndThen {
      assert(start.toFile.exists)
      assert(!f1.toFile.exists)
      assert(!f2.toFile.exists)
      assert(!f3.toFile.exists)

      setup(start, f1, f2, f3)

      val jobResults = executer.execute(executable)

      val jobStates = Seq(startToF1.state, f1ToF2.state, f2ToF3.state)

      assert(jobStates == expectations)

      assert(jobResults.size == expectations.count(_.isSuccess))

      assert(jobResults.values.forall(_.get.isSuccess))
    }
  }

  private lazy val compiler = new LoamCompiler(OutMessageSink.NoOp)

  private def compile(loamCode: String): LExecutable = {

    val compileResults = compiler.compile(loamCode)

    assert(compileResults.errors == Nil)

    val context = compileResults.contextOpt.get

    val mapping = LoamGraphAstMapper.newMapping(context.graph)

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
