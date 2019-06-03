package loamstream.db.slick

import java.nio.file.Path
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.drm.Queue
import loamstream.drm.lsf.LsfDefaults
import loamstream.drm.uger.UgerDefaults
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.ProvidesEnvAndResources
import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.DataHandle.PathHandle
import loamstream.model.jobs.StoreRecord
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.BashScript.Implicits.BashPath
import loamstream.util.Hash
import loamstream.util.Hashes
import loamstream.drm.ContainerParams
import loamstream.model.jobs.TerminationReason
import loamstream.model.execute.Settings

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

  private def noExecutions: Boolean = dao.allExecutions.isEmpty

  import loamstream.TestHelpers.dummyOutputStreams

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
          outputStreams = Option(dummyOutputStreams),
          outputs = outputs.toSet,
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
        outputStreams = Option(dummyOutputStreams), 
        outputs = outputs.toSet,
        terminationReason = Some(TerminationReason.CpuTime))

    dao.insertExecutions(execution)
  }

  test("insert/allExecutions") {
    createTablesAndThen {
      val stored = store(path0)

      assert(stored.outputs.nonEmpty)

      val Seq(retrieved) = dao.allExecutions

      assert(dao.allStoreRecords.toSet === stored.outputs)

      assert(stored === retrieved)
    }
  }

  test("insert/Read Outputs") {
    createTablesAndThen {
      assert(noOutputs)

      store(path0)

      val expected =  Seq(cachedOutput(path0, hash0))

      assert(dao.allStoreRecords === expected)
    }
  }

  test("insert/allOutputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      val expected = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1), cachedOutput(path2, hash2))

      //Use Sets to ignore order
      assert(dao.allStoreRecords.toSet == expected)
    }
  }

  test("insert/allOutputs - only failures") {
    createTablesAndThen {

      assert(noOutputs)

      storeFailures(path0, path1, path2)

      val expected = Set(StoreRecord(path0), StoreRecord(path1), StoreRecord(path2))

      //Use Sets to ignore order
      assert(dao.allStoreRecords.toSet == expected)
      assert(dao.allStoreRecords.forall(dao.findExecution(_).get.isFailure))
    }
  }

  test("insert/allOutputs - some failures") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1)
      storeFailures(path2, path3)

      val failedOutputs = Set(StoreRecord(path2), StoreRecord(path3))
      val hashedOutputs = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1))

      val all: Set[StoreRecord] = failedOutputs ++ hashedOutputs

      //Use Sets to ignore order
      assert(dao.allStoreRecords.toSet == all)
      assert(dao.allStoreRecords.count(dao.findExecution(_).get.isFailure) == failedOutputs.size)
    }
  }

  test("delete all Outputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allStoreRecords.size == 3)

      dao.deletePathOutput(path0, path1, path2)

      assert(noOutputs)
    }
  }

  test("delete some Outputs") {
    createTablesAndThen {

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
    import Helpers.dummyId

    createTablesAndThen {
      assert(noOutputs)
      assert(noExecutions)

      val outputStreams = dummyOutputStreams

      val toInsert = new ExecutionRow(
          dummyId,
          mockUgerSettings.envType.toString,
          mockCmd,
          mockStatus,
          mockExitCode,
          outputStreams.stdout.toString,
          outputStreams.stderr.toString,
          None)
      
      assert(allExecutionRows.isEmpty)

      val insertAction = dao.insertExecutionRow(toInsert)

      dao.runBlocking(insertAction)

      val Seq(recorded) = allExecutionRows

      // The DB must assign an auto-incremented 'id' upon insertion
      assert(recorded.id != dummyId)
      assert(recorded.env === mockUgerSettings.envType.toString)
      assert(recorded.cmd === mockCmd)
      assert(recorded.status === mockStatus)
      assert(recorded.exitCode === mockExitCode)
      assert(recorded.stdoutPath === outputStreams.stdout.toString)
      assert(recorded.stderrPath === outputStreams.stderr.toString)
      assert(recorded.terminationReason === None)
    }
  }
  
  test("insertExecutionRow - with termination reason") {
    import Helpers.dummyId

    createTablesAndThen {
      assert(noOutputs)
      assert(noExecutions)

      val outputStreams = dummyOutputStreams

      val toInsert = new ExecutionRow(
          dummyId,
          mockUgerSettings.envType.toString,
          mockCmd,
          mockStatus,
          mockExitCode,
          outputStreams.stdout.toString,
          outputStreams.stderr.toString,
          Some(TerminationReason.Memory.name))
      
      assert(allExecutionRows.isEmpty)

      val insertAction = dao.insertExecutionRow(toInsert)

      dao.runBlocking(insertAction)

      val Seq(recorded) = allExecutionRows

      // The DB must assign an auto-incremented 'id' upon insertion
      assert(recorded.id != dummyId)
      assert(recorded.env === mockUgerSettings.envType.toString)
      assert(recorded.cmd === mockCmd)
      assert(recorded.status === mockStatus)
      assert(recorded.exitCode === mockExitCode)
      assert(recorded.stdoutPath === outputStreams.stdout.toString)
      assert(recorded.stderrPath === outputStreams.stderr.toString)
      assert(recorded.terminationReason === Some(TerminationReason.Memory.name))
    }
  }

  test("insertExecutions/allExecutionRows") {
    createTablesAndThen {
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
          Some(ContainerParams(imageName = "library/foo:1.2.3")))
              
      val googleSettings = GoogleSettings("some-cluster")

      val localResources = LocalResources(Instant.ofEpochMilli(123), Instant.ofEpochMilli(456))

      val lsfResources = LsfResources(
          Memory.inGb(2.1),
          CpuTime.inSeconds(12.34),
          Some("nodeName"),
          Some(Queue("broad")),
          Instant.ofEpochMilli(64532),
          Instant.ofEpochMilli(9345345))
          
      val googleResources = GoogleResources("clusterName",
        Instant.ofEpochMilli(1), Instant.ofEpochMilli(72345))

      val failed0 = Execution(
          settings = LocalSettings,
          cmd = Option(mockCmd),
          status = CommandResult(42).toJobStatus,
          result = Option(CommandResult(42)),
          resources = Option(localResources),
          outputStreams = Option(dummyOutputStreams), 
          outputs = Set(output0),
          terminationReason = None)

      val failed1 = Execution(
          settings = lsfSettings,
          cmd = Option(mockCmd),
          status = CommandResult(1).toJobStatus,
          result = Option(CommandResult(1)),
          resources = Option(lsfResources),
          outputStreams = Option(dummyOutputStreams), 
          outputs = Set.empty,
          terminationReason = Some(TerminationReason.UserRequested))

      val succeeded = Execution(
          settings = googleSettings,
          cmd = Option(mockCmd),
          status = CommandResult(0).toJobStatus,
          result = Option(CommandResult(0)),
          resources = Option(googleResources),
          outputStreams = Option(dummyOutputStreams),
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
            outputStreams = failed0.outputStreams, 
            outputs = Set(StoreRecord(output0.loc)),
            terminationReason = None)
      }

      assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0))

      dao.insertExecutions(succeeded, failed1)
      val expected1 = failed1
      val expected2 = succeeded
      assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0, expected1, expected2))
    }
  }

  test("insertExecutions - CommandInvocationFailure") {
    def doTest(path: Path): Unit = {
      createTablesAndThen {
        val output0 = PathHandle(path)
  
        val failed = Execution(
            mockUgerSettings,
            mockCmd,
            JobResult.CommandInvocationFailure(new Exception),
            dummyOutputStreams,
            output0.toStoreRecord)
  
        assert(failed.isFailure)
  
        assert(noOutputs)
        assert(noExecutions)
  
        dao.insertExecutions(failed)
  
        val expected0 = Execution(
            mockUgerSettings,
            mockCmd,
            CommandResult(JobResult.DummyExitCode),
            failed.outputStreams.get,
            failedOutput(output0.path))
  
        assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0))
      }
    }
    
    doTest(path0)
    doTest(path("for-hashing/foo.txt"))
  }

  test("insertExecutions - should throw") {
    def doTestWithLocations(path: Path): Unit = {
      def doTest(command: Option[String], jobResult: JobResult): Unit = {
        createTablesAndThen {
          val output0 = PathHandle(path)
  
          val failed = Execution(
              settings = mockUgerSettings,
              cmd = command, 
              status = JobResult.Failure.toJobStatus,
              result = Option(JobResult.Failure),
              resources = None,
              outputStreams = Option(dummyOutputStreams), 
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
  
      doTest(Some(mockCmd), JobResult.Failure)
      doTest(None, JobResult.Failure)
      doTest(Some(mockCmd), JobResult.CommandResult(1))
      doTest(Some(mockCmd), JobResult.CommandResult(1))
      doTest(None, JobResult.CommandResult(0))
    }
    
    doTestWithLocations(path0)
    doTestWithLocations(path("for-hashing/foo.txt"))
  }

  test("findExecution") {
    val ugerSettings = mockUgerSettings
    
    def doTest(resources: Resources): Unit = {
      createTablesAndThen {
        val output0 = cachedOutput(path0, hash0)
        val output1 = cachedOutput(path1, hash1)
        val output2 = cachedOutput(path2, hash2)
  
        val ex0 = Execution(ugerSettings, mockCmd, CommandResult(42), dummyOutputStreams, output0)
        val ex1 = Execution(ugerSettings, mockCmd, CommandResult(0), dummyOutputStreams, output1, output2)
        val ex2 = Execution(
            settings = ugerSettings,
            cmd = Option(mockCmd), 
            status = CommandResult(1).toJobStatus, 
            result = Option(CommandResult(1)),
            resources = Option(resources),
            outputStreams = Option(dummyOutputStreams), 
            outputs = Set.empty,
            terminationReason = None)
  
        assert(ex0.isFailure)
        assert(ex1.isSuccess)
        assert(ex2.isFailure)
        assert(noOutputs)
        assert(noExecutions)
  
        dao.insertExecutions(ex0)
  
        val expected0 = Execution(ugerSettings, mockCmd, CommandResult(42), ex0.outputStreams.get, failedOutput(path0))
  
        assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0))
        assertEqualFieldsFor(dao.findExecution(output0), Some(expected0))
  
        assert(dao.findExecution(output1) === None)
        assert(dao.findExecution(output2) === None)
  
        dao.insertExecutions(ex1, ex2)
  
        val expected1 = ex1
        val expected2 = ex2
  
        assertEqualFieldsFor(dao.allExecutions.toSet, Set(expected0, expected1, expected2))
  
        assertEqualFieldsFor(dao.findExecution(output0), Some(expected0))
        assertEqualFieldsFor(dao.findExecution(output1), Some(expected1))
        assertEqualFieldsFor(dao.findExecution(output2), Some(expected1))
      }
      
      doTest(TestHelpers.localResources)
      doTest(TestHelpers.ugerResources)
      doTest(TestHelpers.lsfResources)
      doTest(TestHelpers.googleResources)
    }
  }

  test("findOutput") {
    createTablesAndThen {
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
    createTablesAndThen {
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
        Some(ContainerParams(imageName = "library/foo:1.2.3")))

    val lsfSettings = LsfDrmSettings(
        Cpus(8), 
        Memory.inGb(4), 
        LsfDefaults.maxRunTime, 
        None, 
        Some(ContainerParams(imageName = "library/foo:1.2.3")))
        
    val googleSettings = GoogleSettings("some-cluster")

    val localResources = LocalResources(Instant.ofEpochMilli(123), Instant.ofEpochMilli(456))

    val ugerResources = UgerResources(
        Memory.inGb(2.1),
        CpuTime.inSeconds(12.34),
        Some("nodeName"),
        Some(Queue("broad")),
        Instant.ofEpochMilli(64532),
        Instant.ofEpochMilli(9345345))
        
    val lsfResources = LsfResources(
        Memory.inGb(1.2),
        CpuTime.inSeconds(34.21),
        Some("another-node"),
        Some(Queue("ebi")),
        Instant.ofEpochMilli(12345),
        Instant.ofEpochMilli(12346))

    val googleResources = GoogleResources(
        "clusterName",
        Instant.ofEpochMilli(1), 
        Instant.ofEpochMilli(72345))

    val output0 = cachedOutput(path0, hash0)
        
    def doTest(settings: Settings, resources: Resources): Unit = {
      createTablesAndThen {
        assert(noExecutions)
        
        val result = CommandResult(0)
        
        val execution = Execution(
            settings = settings,
            cmd = Option(mockCmd),
            status = result.toJobStatus,
            result = Option(result),
            resources = Option(resources),
            outputStreams = Option(dummyOutputStreams), 
            outputs = Set(output0),
            terminationReason = None)
            
        dao.insertExecutions(execution)
        
        import dao.tables.driver.api._
        
        val resourcesFromDb: ResourceRow = dao.runBlocking { 
          settings.envType match {
            case EnvironmentType.Local => dao.tables.localResources.result
            case EnvironmentType.Uger => dao.tables.ugerResources.result
            case EnvironmentType.Lsf => dao.tables.lsfResources.result
            case EnvironmentType.Google => dao.tables.googleResources.result
          }
        }.head
        
        assert(resourcesFromDb === ResourceRow.fromResources(resources, 1))
        
        assert(dao.allExecutions.head === execution)
      }
    }
      
    doTest(localSettings, localResources)
    doTest(ugerSettings, ugerResources)
    doTest(lsfSettings, lsfResources)
    doTest(googleSettings, googleResources)
  }
}
