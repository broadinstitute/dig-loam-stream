package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.Execution
import loamstream.model.jobs.StoreRecord
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.TestHelpers.dummyJobDir
import loamstream.TestHelpers.path
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.MockJob
import loamstream.TestHelpers

/**
 * @author clint
 * Jul 2, 2018
 */
final class DbBackedExecutionRecorderTest extends FunSuite with ProvidesSlickLoamDao with ProvidesEnvAndResources {
  
  private val p0 = path("src/test/resources/for-hashing/foo.txt")
  private val p1 = path("src/test/resources/for-hashing/empty.txt")
  private val p2 = path("src/test/resources/for-hashing/subdir/bar.txt")
  private val nonexistentPath = path("non/existent/blah.txt")

  private val o0 = DataHandle.PathHandle(p0)
  private val o1 = DataHandle.PathHandle(p1)
  private val o2 = DataHandle.PathHandle(p2)
  private val nonExistentOutput = DataHandle.PathHandle(nonexistentPath)
  
  private val cachedOutput0 = o0.toStoreRecord
  private val cachedOutput1 = o1.toStoreRecord
  private val cachedOutput2 = o2.toStoreRecord
  private val cachedNonExistentOutput = nonExistentOutput.toStoreRecord

  private val failedOutput0 = failedOutput(p0)
  private val failedOutput1 = failedOutput(p1)
  private val failedOutput2 = failedOutput(p2)
  
  private val run: Run = Run.create()
  
  test("record() - no Executions") {
    createTablesAndThen {
      val recorder = new DbBackedExecutionRecorder(dao, HashingStrategy.HashOutputs)

      assert(executions === Nil)

      recorder.record(TestHelpers.DummyJobOracle, Nil)

      assert(executions === Nil)
    }
  }

  test("record() - non-command-Execution") {
    def doTest(
        command: Option[String], 
        status: JobStatus, 
        result: Option[JobResult] = None): Unit = {
      
      def doIt(hashingStrategy: HashingStrategy): Unit = {
        registerRunAndThen(run) {
          val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
    
          assert(executions === Nil)
    
          val job = MockJob(status)
          
          val e = Execution(
              settings = mockUgerSettings,
              cmd = command, 
              status = status, 
              result = result, 
              resources = Option(mockResources),
              jobDir = None,
              outputs = Set.empty[StoreRecord],
              terminationReason = None)
  
          assert(e.isPersistable === (status.isSkipped || command.isDefined))
          
          recorder.record(TestHelpers.DummyJobOracle, Seq(job -> e))
          
          assert(executions === Nil)
        }
      }
      
      doIt(HashingStrategy.DontHashOutputs)
      doIt(HashingStrategy.HashOutputs)
    }
    
    doTest(None, JobStatus.Succeeded, Option(CommandResult(0)))
    doTest(None, JobStatus.Failed, Option(JobResult.Failure))
    doTest(None, JobStatus.Succeeded)
  }

  test("record() - successful command-Execution, no outputs") {
    def doTest(hashingStrategy: HashingStrategy): Unit = {
      registerRunAndThen(run) {
        val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
  
        assert(executions === Nil)
  
        val cr = CommandResult(0)
  
        assert(cr.isSuccess)
  
        val job = MockJob(cr.toJobStatus)
        
        val e = Execution(
            settings = mockUgerSettings,
            cmd = Option(mockCmd), 
            status = cr.toJobStatus, 
            result = Option(cr), 
            resources = Option(mockResources),
            jobDir = Some(dummyJobDir),
            outputs = Set.empty[StoreRecord],
            terminationReason = None)
  
        recorder.record(TestHelpers.DummyJobOracle, Seq(job -> e))
  
        assertEqualFieldsFor(executions, Seq(e))
      }
    }
    
    doTest(HashingStrategy.DontHashOutputs)
    doTest(HashingStrategy.HashOutputs)
  }
  
  test("record() - skipped command-Execution, no outputs") {
    def doTest(hashingStrategy: HashingStrategy): Unit = {
      registerRunAndThen(run) {
        val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
  
        assert(executions === Nil)
  
        val cr = JobResult.Success
  
        assert(cr.isSuccess)
  
        val job = MockJob(cr.toJobStatus)
        
        val e = Execution(
            settings = mockUgerSettings,
            cmd = Option(mockCmd), 
            status = JobStatus.Skipped, 
            result = Option(cr), 
            resources = Option(mockResources),
            jobDir = Some(dummyJobDir),
            outputs = Set.empty[StoreRecord],
            terminationReason = None)
  
        recorder.record(TestHelpers.DummyJobOracle, Seq(job -> e))
        
        val expected = e.copy(result = Some(CommandResult(0)))
  
        assertEqualFieldsFor(executions, Seq(expected))
      }
    }
    
    doTest(HashingStrategy.DontHashOutputs)
    doTest(HashingStrategy.HashOutputs)
  }

  test("record() - failed command-Execution, no outputs") {
    def doTest(hashingStrategy: HashingStrategy): Unit = {
      registerRunAndThen(run) {
        val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
  
        assert(executions === Nil)
  
        val cr = CommandResult(42)
  
        assert(cr.isFailure)
  
        val job = MockJob(cr.toJobStatus)
        
        val e = Execution(
            settings = mockUgerSettings,
            cmd = Option(mockCmd), 
            status = cr.toJobStatus, 
            result = Option(cr),
            resources = Option(mockResources),
            jobDir = Some(dummyJobDir),
            outputs = Set.empty,
            terminationReason = None)
  
        recorder.record(TestHelpers.DummyJobOracle, Seq(job -> e))
  
        assertEqualFieldsFor(executions, Seq(e))
      }
    }
    
    doTest(HashingStrategy.DontHashOutputs)
    doTest(HashingStrategy.HashOutputs)
  }

  test("record() - successful command-Execution, some outputs") {
    def doTest(hashingStrategy: HashingStrategy): Unit = {
      registerRunAndThen(run) {
        val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
  
        assert(executions === Nil)
  
        val cr = CommandResult(0)
  
        assert(cr.isSuccess)
  
        val job = MockJob(cr.toJobStatus)
        
        val e = Execution.fromOutputs(mockUgerSettings, mockCmd, cr, dummyJobDir, Set(o0, o1, o2))
  
        val baseStoreRecords = Iterable(cachedOutput0, cachedOutput1, cachedOutput2)
        
        val storeRecords = {
          if(hashingStrategy.shouldHash) { baseStoreRecords }
          else { baseStoreRecords.map(noHash) }
        }
        
        val withHashedOutputs = e.withStoreRecords(storeRecords.toSet)
  
        recorder.record(TestHelpers.DummyJobOracle, Seq(job -> e))
  
        assertEqualFieldsFor(executions, Seq(withHashedOutputs))
      }
    }
    
    doTest(HashingStrategy.DontHashOutputs)
    doTest(HashingStrategy.HashOutputs)
  }
  
  private def noHash(sr: StoreRecord): StoreRecord = sr.copy(makeHash = () => None, makeHashType = () => None)
  
  test("record() - skipped command-Execution, some outputs") {
    def doTest(hashingStrategy: HashingStrategy): Unit = {
      registerRunAndThen(run) {
        val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
  
        assert(executions === Nil)
  
        val cr = JobResult.Success
  
        assert(cr.isSuccess)
  
        val job = MockJob(JobStatus.Skipped)
        
        val e = Execution.fromOutputs(mockUgerSettings, mockCmd, cr, dummyJobDir, Set(o0, o1, o2))
  
        recorder.record(TestHelpers.DummyJobOracle, Seq(job -> e))
  
        val baseStoreRecords = Iterable(cachedOutput0, cachedOutput1, cachedOutput2)
        
        val storeRecords = {
          if(hashingStrategy.shouldHash) { baseStoreRecords }
          else { baseStoreRecords.map(noHash) }
        }
        
        val withHashedOutputs = e.withStoreRecords(storeRecords.toSet)
        
        val expected = withHashedOutputs.copy(result = Some(CommandResult(0)))
        
        assertEqualFieldsFor(executions, Seq(expected))
      }
    }
    
    doTest(HashingStrategy.DontHashOutputs)
    doTest(HashingStrategy.HashOutputs)
  }

  test("record() - failed command-Execution, some outputs") {
    def doTest(hashingStrategy: HashingStrategy): Unit = {
      registerRunAndThen(run) {
        val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
  
        assert(executions === Nil)
  
        val cr = CommandResult(42)
  
        assert(cr.isFailure)
  
        val job = MockJob(cr.toJobStatus)
        
        val e = Execution.fromOutputs(mockUgerSettings, mockCmd, cr, dummyJobDir, Set[DataHandle](o0, o1, o2))
  
        recorder.record(TestHelpers.DummyJobOracle, Seq(job -> e))
  
        val expected = Seq(
            Execution(
                settings = mockUgerSettings, 
                cmd = mockCmd, 
                result = CommandResult(42),
                jobDir = e.jobDir.get,
                outputs = failedOutput0, failedOutput1, failedOutput2))
        
        assertEqualFieldsFor(executions, expected)
      }
    }
    
    doTest(HashingStrategy.DontHashOutputs)
    doTest(HashingStrategy.HashOutputs)
  }
  
  test("record() - successful native job Execution, some outputs") {
    def doTest(hashingStrategy: HashingStrategy): Unit = {
      registerRunAndThen(run) {
        val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
  
        assert(executions === Nil)
  
        val cr = JobResult.Success
  
        assert(cr.isSuccess)
  
        val job = MockJob(cr.toJobStatus)
        
        val e = Execution.fromOutputs(
            settings = LocalSettings, 
            cmd = "native-job-name", 
            result = JobResult.CommandResult(0), 
            jobDir = dummyJobDir, 
            outputs = Set(o0, o1, o2))
            
        assert(e.isPersistable)
  
        val baseStoreRecords = Iterable(cachedOutput0, cachedOutput1, cachedOutput2)
        
        val storeRecords = {
          if(hashingStrategy.shouldHash) { baseStoreRecords }
          else { baseStoreRecords.map(noHash) }
        }
        
        val expected = e.withStoreRecords(storeRecords.toSet)
  
        recorder.record(TestHelpers.DummyJobOracle, Seq(job -> e))
        
        assertEqualFieldsFor(executions, Seq(expected))
      }
    }
    
    doTest(HashingStrategy.DontHashOutputs)
    doTest(HashingStrategy.HashOutputs)
  }
  
  test("record() - failed native job Execution, some outputs") {
    def doTest(hashingStrategy: HashingStrategy): Unit = {
      registerRunAndThen(run) {
        val recorder = new DbBackedExecutionRecorder(dao, hashingStrategy)
  
        assert(executions === Nil)
  
        val cr = {
          val e = new Exception with scala.util.control.NoStackTrace
          
          JobResult.FailureWithException(e)
        }
  
        assert(cr.isFailure)
  
        val job = MockJob(cr.toJobStatus)
        
        val execution = Execution.fromOutputs(mockUgerSettings, mockCmd, cr, dummyJobDir, Set[DataHandle](o0, o1, o2))
  
        assert(execution.isPersistable)
        
        recorder.record(TestHelpers.DummyJobOracle, Seq(job -> execution))
  
        val expected = Seq(
            Execution(
                settings = mockUgerSettings, 
                cmd = mockCmd, 
                result = CommandResult(JobResult.DummyExitCode),
                jobDir = execution.jobDir.get,
                outputs = failedOutput0, failedOutput1, failedOutput2))
        
        assertEqualFieldsFor(executions, expected)
      }
    }
    
    doTest(HashingStrategy.DontHashOutputs)
    doTest(HashingStrategy.HashOutputs)
  }
}
