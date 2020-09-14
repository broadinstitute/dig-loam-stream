package loamstream.model.execute

import java.nio.file.Path
import java.nio.file.StandardCopyOption

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobStatus.Skipped
import loamstream.model.jobs.JobStatus.Succeeded
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.RunData
import loamstream.util.Hashes
import loamstream.util.Paths

/**
  * @author clint
  *         kaan
  *         date: Aug 12, 2016
  */
final class ExecutionResumptionTest extends FunSuite with ProvidesSlickLoamDao with ProvidesEnvAndResources {
  private def runsEverythingExecuter = RxExecuter.default

  private def dbBackedExecuter = RxExecuter.defaultWith(new DbBackedJobFilter(dao))

  private def hashAndStore(p: Path, exitCode: Int = 0): Execution = {
    val hash = Hashes.sha1(p)
    val lastModified = Paths.lastModifiedTime(p)

    val e = Execution(
        settings = mockUgerSettings,
        cmd = Option(mockCmd),
        status = JobStatus.fromExitCode(exitCode),
        result = Option(JobResult.CommandResult(exitCode)),
        resources = Some(mockResources),
        jobDir = Some(TestHelpers.dummyJobDir),
        outputs = Set(cachedOutput(p, hash, lastModified)),
        terminationReason = None)
    
    store(e)
    
    e
  }
  
  private val run: Run = Run.create()

  test("Pipelines can be resumed after stopping 1/3rd of the way through") {
    doTest(Seq(Skipped, Succeeded, Succeeded)) { (start, f1, f2, f3) =>
      import java.nio.file.{Files => JFiles}

      assert(!f1.toFile.exists)
      
      JFiles.copy(start, f1)
      
      hashAndStore(f1)
      
      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))
    }
  }

  test("Pipelines can be resumed after stopping 2/3rds of the way through") {
    doTest(Seq(Skipped, Skipped, Succeeded)) { (start, f1, f2, f3) =>
      import java.nio.file.{Files => JFiles}

      JFiles.copy(start, f1)
      JFiles.copy(start, f2)

      hashAndStore(f1)
      hashAndStore(f2)

      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))

      assert(f2.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f2))
    }
  }

  test("Re-running a finished pipelines does nothing") {
    doTest(Seq(Skipped, Skipped, Skipped)) { (start, f1, f2, f3) =>
      import java.nio.file.{Files => JFiles}

      JFiles.copy(start, f1)
      JFiles.copy(start, f2)
      JFiles.copy(start, f3)

      hashAndStore(f1)
      hashAndStore(f2)
      hashAndStore(f3)

      assert(f1.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f1))

      assert(f2.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f2))

      assert(f3.toFile.exists)
      assert(Hashes.sha1(start) == Hashes.sha1(f3))
    }
  }

  test("Every job is run for Pipelines with no existing outputs") {
    doTest(Seq(Succeeded, Succeeded, Succeeded)) { (start, f1, f2, f3) =>
      //No setup
    }
  }

  private def mockJob(
      name: String, 
      outputs: Set[DataHandle],
      inputs: Set[DataHandle] = Set.empty,
      dependencies: Set[JobNode] = Set.empty)(body: => Any): MockJob = {
    
    new MockJob.FromJobFn(
        toReturnFn = job => TestHelpers.runDataFromResult(job, LocalSettings, JobResult.CommandResult(0)), 
        name = name, 
        dependencies = dependencies,
        successorsFn = () => Set.empty,
        inputs = inputs,
        outputs = outputs, 
        delay = 0) {
      
      override def execute(implicit context: ExecutionContext): Future[RunData] = {
        body
        
        super.execute
      }
    }
  }
  
  private def makeMockJobs(start: Path, f1: Path, f2: Path, f3: Path): (MockJob, MockJob, MockJob) = {

    val startToF1 = mockJob(copyCmd(start, f1), Set(DataHandle.PathHandle(f1))) {
      copy(start, f1)
    }

    val f1ToF2 = mockJob(copyCmd(f1, f2), Set(DataHandle.PathHandle(f2)), dependencies = Set(startToF1)) {
      copy(f1, f2)
    }

    val f2ToF3 = mockJob(copyCmd(f2, f3), Set(DataHandle.PathHandle(f3)), dependencies = Set(f1ToF2)) {
      copy(f2, f3)
    }
    
    (startToF1, f1ToF2, f2ToF3)
  }
  
  private type SetupFn = (Path, Path, Path, Path) => Any 
  
  private def runWithExecuter(
      runningEverything: Boolean,
      executer: RxExecuter,
      executable: Executable,
      mockJobs: (MockJob, MockJob, MockJob),
      expectations: Seq[JobStatus],
      doSetup: () => Unit): Unit = {
    
    registerRunAndThen(run) {
      if (!runningEverything) {
        doSetup()
      }

      val jobResults = executer.execute(executable)

      val (startToF1, f1ToF2, f2ToF3) = mockJobs
      
      val jobStatuses = Seq(jobResults(startToF1).status, jobResults(f1ToF2).status, jobResults(f2ToF3).status)

      val expectedStatuses = if (runningEverything) Seq(Succeeded, Succeeded, Succeeded) else expectations

      assert(jobStatuses == expectedStatuses)

      val expectedNumResults = if (runningEverything) 3 else expectations.count(_.isSuccess)

      assert(jobResults.size == expectedNumResults)

      assert(jobResults.values.forall(_.isSuccess))
    }
  }
  
  private def doTestWithExecuter(executer: RxExecuter, expectations: Seq[JobStatus], setup: SetupFn): Unit = {
    import loamstream.TestHelpers.path
    import loamstream.util.Paths.Implicits.PathHelpers

    val start = path("src/test/resources/a.txt")
    
    def doTest(
        makeF1: Path => Path, 
        makeF2: Path => Path, 
        makeF3: Path => Path): Unit = withWorkDir { workDir =>
        
      val (f1, f2, f3) = (makeF1(workDir), makeF2(workDir), makeF3(workDir))

      val mockJobsTuple = makeMockJobs(start, f1, f2, f3)
      
      val (startToF1, f1ToF2, f2ToF3) = mockJobsTuple 

      val executable = Executable(Set(f2ToF3))
  
      val runningEverything: Boolean = executer.jobFilter == JobFilter.RunEverything

      import java.nio.file.Files.exists
      
      assert(exists(start))
      assert(exists(f1) === false)
      assert(exists(f2) === false)
      assert(exists(f3) === false)
      
      runWithExecuter(
          runningEverything,
          executer,
          executable,
          mockJobsTuple,
          expectations, 
          () => setup(start, f1, f2, f3))
          
      assert(exists(start))
      assert(exists(f1))
      assert(exists(f2))
      assert(exists(f3))
    }
    
    doTest(_ / "fileOut1.txt", _ / "fileOut2.txt", _ / "fileOut3.txt")
  }

  //NB: Tests with the 'run-everything' JobFilter as well as a DB-backed one.
  private def doTest(expectations: Seq[JobStatus])(setup: SetupFn): Unit = {
    doTestWithExecuter(runsEverythingExecuter, expectations, setup)

    doTestWithExecuter(dbBackedExecuter, expectations, setup)
  }

  // Setting the option to replace existing files so if a 'cp' job was mistakenly run (instead of being skipped),
  // the test fails with a more descriptive message instead of a FileAlreadyExistsException
  private def copy(source: Path, target: Path): Unit = {
    java.nio.file.Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
  }
  
  private def withWorkDir[A](body: Path => A): A = TestHelpers.withWorkDir(getClass.getSimpleName)(body)
  
  private def copyCmd(file1: Path, file2: Path): String = s"cp $file1 $file2"
}
