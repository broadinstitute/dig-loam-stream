package loamstream.db.slick

import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.drm.ContainerParams
import loamstream.drm.Queue
import loamstream.drm.lsf.LsfDefaults
import loamstream.drm.uger.UgerDefaults
import loamstream.googlecloud.ClusterConfig
import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.ProvidesEnvAndResources
import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.execute.Run
import loamstream.model.execute.Settings
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.jobs.DataHandle.PathHandle
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.StoreRecord
import loamstream.model.jobs.TerminationReason
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.BashScript.Implicits.BashPath
import loamstream.util.Hashes

import scala.collection.compat._

/**
 * @author clint
 *         kyuksel
 *         date: 8/9/16
 */
final class SlickLoamDaoTest extends FunSuite with ProvidesSlickLoamDao with ProvidesEnvAndResources {
  import loamstream.TestHelpers.path
  
  private val srcTestResources = path("src/test/resources")
  
  private val resolveInSrcTestResources: Path => Path = srcTestResources.resolve(_)
  
  private val path0 = path("src/test/resources/for-hashing/foo.txt")
  private val path1 = path("src/test/resources/for-hashing/bigger")
  private val path2 = path("src/test/resources/for-hashing/empty.txt")
  private val path3 = path("src/test/resources/blarg")

  private lazy val hash0 = Hashes.sha1(path0)
  private lazy val hash1 = Hashes.sha1(path1)
  private lazy val hash2 = Hashes.sha1(path2)

  private val cmd0 = "(echo 10 ; sed '1d' ancestry.txt | cut -f5- | sed 's/\t/ /g') > ancestry.fet"
  private val cmd1 = "R --vanilla --args pca_scores.tsv < plot_pca.r"

  private def noOutputs: Boolean = dao.allStoreRecords.isEmpty

  private def noExecutions: Boolean = executions.isEmpty

  import loamstream.TestHelpers.dummyJobDir

  private val run: Run = Run.create()
  
  private def store(cmd: String, paths: Path*): Execution = {
    val outputs = paths.map { path =>
      val hash = Hashes.sha1(path)

      cachedOutput(path, hash)
    }

    val execution = {
      val result = JobResult.CommandResult(0)
      
      Execution(
          settings = mockLsfSettings,
          cmd = Option(cmd),
          status = result.toJobStatus,
          result = Option(result),
          resources = Option(TestHelpers.lsfResources),
          jobDir = Option(dummyJobDir),
          outputs = outputs.to(Set),
          terminationReason = None)
    }

    dao.insertExecutions(execution)
    
    execution
  }

  private def allExecutionRows: Seq[ExecutionRow] = {
    val driver = dao.descriptor.dbType.driver

    import driver.api._

    dao.runBlocking(dao.tables.executions.result)
  }

  private def store(paths: Path*): Execution = {
    store(mockCmd, paths:_*)
  }

  private def storeFailures(paths: Path*): Unit = {
    val outputs = paths.map(StoreRecord(_))

    //NB: Failure
    val result = JobResult.CommandResult(42)

    assert(result.isFailure)

    val execution = Execution(
        settings = mockUgerSettings,
        cmd = Option(mockCmd), 
        status = result.toJobStatus,
        result = Option(result), 
        resources = Option(TestHelpers.ugerResources), 
        jobDir = Option(dummyJobDir), 
        outputs = outputs.to(Set),
        terminationReason = Some(TerminationReason.CpuTime))

    dao.insertExecutions(execution)
  }
  
  test("findLastRun - empty DB") {
    createTablesAndThen {
      assert(dao.findLastRun === None)
    }
  }
  
  private def allRunsByDate: Seq[Run] = {
    import dao.driver.api._
    
    dao.runBlocking(dao.tables.runs.sortBy(_.timeStamp).result).map(_.toRun)
  }
  
  test("findLastRun / registerNewRun") {
    createTablesAndThen {
      assert(dao.findLastRun === None)
      
      val r0 = Run.create()
      
      dao.registerNewRun(r0)
      
      assert(allRunsByDate === Seq(r0))
      
      val r1 = Run.create()
      
      dao.registerNewRun(r1)
      
      assert(allRunsByDate === Seq(r0, r1))
      
      val r2 = Run.create()
      
      dao.registerNewRun(r2)
      
      assert(allRunsByDate === Seq(r1, r2))
      
      val r3 = Run.create()
      
      dao.registerNewRun(r3)
      
      assert(allRunsByDate === Seq(r2, r3))
    }
  }

  test("insert/allExecutions") {
    registerRunAndThen(run) {
      val stored = store(path0)

      assert(stored.outputs.nonEmpty)

      val Seq(retrieved) = executions

      assert(dao.allStoreRecords.to(Set) === stored.outputs)

      assert(stored === retrieved)
    }
  }

  test("insert/Read Outputs") {
    registerRunAndThen(run) {
      assert(noOutputs)

      store(path0)

      val expected =  Seq(cachedOutput(path0, hash0))

      assert(dao.allStoreRecords === expected)
    }
  }

  test("insert/allOutputs") {
    registerRunAndThen(run) {

      assert(noOutputs)

      store(path0, path1, path2)

      val expected = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1), cachedOutput(path2, hash2))

      //Use Sets to ignore order
      assert(dao.allStoreRecords.to(Set) == expected)
    }
  }

  test("insert/allOutputs - only failures") {
    registerRunAndThen(run) {

      assert(noOutputs)

      storeFailures(path0, path1, path2)

      val expected = Set(StoreRecord(path0), StoreRecord(path1), StoreRecord(path2))

      //Use Sets to ignore order
      assert(dao.allStoreRecords.to(Set) == expected)
      assert(dao.allStoreRecords.forall(dao.findLastStatus(_).get.isFailure))
    }
  }

  test("insert/allOutputs - some failures") {
    registerRunAndThen(run) {

      assert(noOutputs)

      store(path0, path1)
      storeFailures(path2, path3)

      val failedOutputs = Set(StoreRecord(path2), StoreRecord(path3))
      val hashedOutputs = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1))

      val all: Set[StoreRecord] = failedOutputs ++ hashedOutputs

      //Use Sets to ignore order
      assert(dao.allStoreRecords.to(Set) == all)
      assert(dao.allStoreRecords.count(dao.findLastStatus(_).get.isFailure) == failedOutputs.size)
    }
  }

  test("delete all Outputs") {
    registerRunAndThen(run) {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allStoreRecords.size == 3)

      dao.deletePathOutput(path0, path1, path2)

      assert(noOutputs)
    }
  }

  test("delete some Outputs") {
    registerRunAndThen(run) {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allStoreRecords.size == 3)

      dao.deletePathOutput(path0, path2)

      assert(dao.allStoreRecords.map(_.loc) == Seq(path1.toAbsolutePath.render))

      dao.deletePathOutput(path0, path1)

      assert(noOutputs)
    }
  }

  test("insertExecutionRow - no termination reason") {
    import DbHelpers.dummyId

    registerRunAndThen(run) {
      assert(noOutputs)
      assert(noExecutions)

      val jobDir = dummyJobDir

      val toInsert = new ExecutionRow(
          dummyId,
          mockUgerSettings.envType.toString,
          Option(mockCmd),
          mockStatus,
          mockExitCode,
          Some(jobDir.toString),
          None,
          //TODO: mega hack
          runId = dao.findLastRunId)
      
      assert(allExecutionRows.isEmpty)

      val insertAction = dao.insertExecutionRow(toInsert)

      dao.runBlocking(insertAction)

      val Seq(recorded) = allExecutionRows

      // The DB must assign an auto-incremented 'id' upon insertion
      assert(recorded.id != dummyId)
      assert(recorded.env === mockUgerSettings.envType.toString)
      assert(recorded.cmd === Some(mockCmd))
      assert(recorded.status === mockStatus)
      assert(recorded.exitCode === mockExitCode)
      assert(recorded.jobDir === Some(jobDir.toString))
      assert(recorded.terminationReason === None)
    }
  }
  
  test("insertExecutionRow - with termination reason") {
    import DbHelpers.dummyId

    registerRunAndThen(run) {
      assert(noOutputs)
      assert(noExecutions)

      val jobDir = dummyJobDir

      val toInsert = new ExecutionRow(
          dummyId,
          mockUgerSettings.envType.toString,
          Option(mockCmd),
          mockStatus,
          mockExitCode,
          Some(jobDir.toString),
          Some(TerminationReason.Memory.name),
          //TODO: mega hack
          runId = dao.findLastRunId)
      
      assert(allExecutionRows.isEmpty)

      val insertAction = dao.insertExecutionRow(toInsert)

      dao.runBlocking(insertAction)

      val Seq(recorded) = allExecutionRows

      // The DB must assign an auto-incremented 'id' upon insertion
      assert(recorded.id != dummyId)
      assert(recorded.env === mockUgerSettings.envType.toString)
      assert(recorded.cmd === Some(mockCmd))
      assert(recorded.status === mockStatus)
      assert(recorded.exitCode === mockExitCode)
      assert(recorded.jobDir === Some(jobDir.toString))
      assert(recorded.terminationReason === Some(TerminationReason.Memory.name))
    }
  }

  test("insertExecutions/allExecutionRows") {
    registerRunAndThen(run) {
      import TestHelpers.path
      
      val output0 = cachedOutput(path0, hash0)
      val output1 = cachedOutput(path1, hash1)
      val output2 = cachedOutput(path2, hash2)

      val localSettings = LocalSettings
      val lsfSettings = LsfDrmSettings(
          Cpus(8), 
          Memory.inGb(4), 
          LsfDefaults.maxRunTime, 
          None,
          Some(ContainerParams(imageName = "library/foo:1.2.3", "")))
              
      val googleSettings = GoogleSettings("some-cluster", ClusterConfig.default)

      val localResources = LocalResources(
          TestHelpers.toLocalDateTime(123), 
          TestHelpers.toLocalDateTime(456))

      val lsfResources = LsfResources(
          Memory.inGb(2.1),
          CpuTime.inSeconds(12.34),
          Some("nodeName"),
          Some(Queue("broad")),
          TestHelpers.toLocalDateTime(64532),
          TestHelpers.toLocalDateTime(9345345))
          
      val googleResources = GoogleResources("clusterName",
        TestHelpers.toLocalDateTime(1), 
        TestHelpers.toLocalDateTime(72345))

      val failed0 = Execution(
          settings = LocalSettings,
          cmd = Option(mockCmd),
          status = CommandResult(42).toJobStatus,
          result = Option(CommandResult(42)),
          resources = Option(localResources),
          jobDir = Option(dummyJobDir), 
          outputs = Set(output0),
          terminationReason = None)

      val failed1 = Execution(
          settings = lsfSettings,
          cmd = Option(mockCmd),
          status = CommandResult(1).toJobStatus,
          result = Option(CommandResult(1)),
          resources = Option(lsfResources),
          jobDir = Option(dummyJobDir), 
          outputs = Set.empty,
          terminationReason = Some(TerminationReason.UserRequested))

      val succeeded = Execution(
          settings = googleSettings,
          cmd = Option(mockCmd),
          status = CommandResult(0).toJobStatus,
          result = Option(CommandResult(0)),
          resources = Option(googleResources),
          jobDir = Option(dummyJobDir),
          outputs = Set(output1, output2),
          terminationReason = None)

      assert(failed0.isFailure)
      assert(failed1.isFailure)
      assert(succeeded.isSuccess)

      assert(noOutputs)
      assert(noExecutions)

      dao.insertExecutions(failed0)

      val expected0 = {
        Execution(
            settings = LocalSettings,
            cmd = Option(mockCmd), 
            status = CommandResult(42).toJobStatus, 
            result = Option(CommandResult(42)),
            resources = Option(localResources),
            jobDir = failed0.jobDir, 
            outputs = Set(StoreRecord(output0.loc)),
            terminationReason = None)
      }

      assertEqualFieldsFor(executions.to(Set), Set(expected0))

      dao.insertExecutions(succeeded, failed1)
      val expected1 = failed1
      val expected2 = succeeded
      assertEqualFieldsFor(executions.to(Set), Set(expected0, expected1, expected2))
    }
  }

  test("insertExecutions - CommandInvocationFailure") {
    def doTest(path: Path): Unit = {
      registerRunAndThen(run) {
        val output0 = PathHandle(path)
  
        val failed = Execution(
            mockUgerSettings,
            mockCmd,
            JobResult.CommandInvocationFailure(new Exception),
            dummyJobDir,
            output0.toStoreRecord)
  
        assert(failed.isFailure)
  
        assert(noOutputs)
        assert(noExecutions)
  
        dao.insertExecutions(failed)
  
        val expected0 = Execution(
            mockUgerSettings,
            mockCmd,
            CommandResult(JobResult.DummyExitCode),
            failed.jobDir.get,
            failedOutput(output0.path))
  
        assertEqualFieldsFor(executions.to(Set), Set(expected0))
      }
    }
    
    doTest(path0)
    doTest(path("for-hashing/foo.txt"))
  }

  test("insertExecutions - should throw") {
    def doTestWithLocations(path: Path): Unit = {
      def doTest(command: Option[String], jobResult: JobResult): Unit = {
        registerRunAndThen(run) {
          val output0 = PathHandle(path)
  
          val failed = Execution(
              settings = mockUgerSettings,
              cmd = command, 
              status = JobResult.Failure.toJobStatus,
              result = Option(JobResult.Failure),
              resources = None,
              jobDir = Option(dummyJobDir), 
              outputs = Set(output0.toStoreRecord),
              terminationReason = None)
  
          assert(failed.isFailure)
  
          assert(noOutputs)
          assert(noExecutions)
  
          intercept[Exception] {
            dao.insertExecutions(failed)
          }
  
          assert(noOutputs)
          assert(noExecutions)
        }
      }
  
      doTest(None, JobResult.Failure)
      doTest(None, JobResult.CommandResult(0))
    }
    
    doTestWithLocations(path0)
    doTestWithLocations(path("for-hashing/foo.txt"))
  }

  test("findExecution") {
    val ugerSettings = mockUgerSettings
    
    def doTest(resources: Resources): Unit = {
      registerRunAndThen(run) {
        val output0 = cachedOutput(path0, hash0)
        val output1 = cachedOutput(path1, hash1)
        val output2 = cachedOutput(path2, hash2)
  
        val ex0 = Execution(ugerSettings, mockCmd, CommandResult(42), dummyJobDir, output0)
        val ex1 = Execution(ugerSettings, mockCmd, CommandResult(0), dummyJobDir, output1, output2)
        val ex2 = Execution(
            settings = ugerSettings,
            cmd = Option(mockCmd), 
            status = CommandResult(1).toJobStatus, 
            result = Option(CommandResult(1)),
            resources = Option(resources),
            jobDir = Option(dummyJobDir), 
            outputs = Set.empty,
            terminationReason = None)
  
        assert(ex0.isFailure)
        assert(ex1.isSuccess)
        assert(ex2.isFailure)
        assert(noOutputs)
        assert(noExecutions)
  
        dao.insertExecutions(ex0)
  
        val expected0 = Execution(ugerSettings, mockCmd, CommandResult(42), ex0.jobDir.get, failedOutput(path0))
  
        assertEqualFieldsFor(executions.to(Set), Set(expected0))
        assertEqualFieldsFor(findExecution(output0), Some(expected0))
  
        assert(findExecution(output1) === None)
        assert(findExecution(output2) === None)
  
        dao.insertExecutions(ex1, ex2)
  
        val expected1 = ex1
        val expected2 = ex2
  
        assertEqualFieldsFor(executions.to(Set), Set(expected0, expected1, expected2))
  
        assertEqualFieldsFor(findExecution(output0), Some(expected0))
        assertEqualFieldsFor(findExecution(output1), Some(expected1))
        assertEqualFieldsFor(findExecution(output2), Some(expected1))
      }
      
      doTest(TestHelpers.localResources)
      doTest(TestHelpers.ugerResources)
      doTest(TestHelpers.lsfResources)
      doTest(TestHelpers.googleResources)
    }
  }

  test("findOutput") {
    registerRunAndThen(run) {
      assert(noOutputs)

      store(path0)

      val outputRecord0FromDb = dao.findStoreRecord(PathHandle(path0).toStoreRecord)
      
      assert(outputRecord0FromDb === Some(cachedOutput(path0, hash0)))

      assert(dao.findStoreRecord(path1) === None)

      storeFailures(path1)

      assert(dao.findStoreRecord(path0) === Some(cachedOutput(path0, hash0)))

      assert(dao.findStoreRecord(path1) === Some(failedOutput(path1)))
    }
  }

  test("findCommand") {
    registerRunAndThen(run) {
      assert(noOutputs)

      store(cmd0, path0)

      assert(dao.findCommand(path0) === Some(cmd0))

      assert(dao.findCommand(path1) === None)

      store(cmd1, path1, path2)

      assert(dao.findCommand(path0) === Some(cmd0))

      assert(dao.findCommand(path1) === Some(cmd1))
      assert(dao.findCommand(path2) === Some(cmd1))
    }
  }
  
  test("Insert/retrieve successful Execution for all environments") {
    import TestHelpers.path
  
    val localSettings = LocalSettings
    
    val ugerSettings = UgerDrmSettings(
        Cpus(8), 
        Memory.inGb(4), 
        UgerDefaults.maxRunTime, 
        Option(UgerDefaults.queue), 
        Some(ContainerParams(imageName = "library/foo:1.2.3", "")))

    val lsfSettings = LsfDrmSettings(
        Cpus(8), 
        Memory.inGb(4), 
        LsfDefaults.maxRunTime, 
        None, 
        Some(ContainerParams(imageName = "library/foo:1.2.3", "")))
        
    val googleSettings = GoogleSettings("some-cluster", ClusterConfig.default)

    val localResources = LocalResources(
        TestHelpers.toLocalDateTime(123), 
        TestHelpers.toLocalDateTime(456))

    val ugerResources = UgerResources(
        Memory.inGb(2.1),
        CpuTime.inSeconds(12.34),
        Some("nodeName"),
        Some(Queue("broad")),
        TestHelpers.toLocalDateTime(64532),
        TestHelpers.toLocalDateTime(9345345))
        
    val lsfResources = LsfResources(
        Memory.inGb(1.2),
        CpuTime.inSeconds(34.21),
        Some("another-node"),
        Some(Queue("ebi")),
        TestHelpers.toLocalDateTime(12345),
        TestHelpers.toLocalDateTime(12346))

    val googleResources = GoogleResources(
        "clusterName",
        TestHelpers.toLocalDateTime(1), 
        TestHelpers.toLocalDateTime(72345))

    val output0 = cachedOutput(path0, hash0)
        
    def doTest(settings: Settings, resources: Resources): Unit = {
      registerRunAndThen(run) {
        assert(noExecutions)
        
        val result = CommandResult(0)
        
        val execution = Execution(
            settings = settings,
            cmd = Option(mockCmd),
            status = result.toJobStatus,
            result = Option(result),
            resources = Option(resources),
            jobDir = Option(dummyJobDir), 
            outputs = Set(output0),
            terminationReason = None)
            
        dao.insertExecutions(execution)
        
        assert(executions.head === execution)
      }
    }
      
    doTest(localSettings, localResources)
    doTest(ugerSettings, ugerResources)
    doTest(lsfSettings, lsfResources)
    doTest(googleSettings, googleResources)
  }
}
