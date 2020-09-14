package loamstream.model.execute

import java.nio.file.Path
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.StoreRecord
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.HashType.Sha1
import loamstream.util.Paths


/**
 * @author clint
 *         date: Sep 30, 2016
 */
final class DbBackedJobFilterTest extends FunSuite with ProvidesSlickLoamDao with ProvidesEnvAndResources {

  import TestHelpers.path
  import TestHelpers.dummyJobDir
  
  private val nonexistentPath = path("non/existent/blah.txt")

  private trait Outputs {
    def p0: Path
    def p1: Path
    def p2: Path
    
    def o0: DataHandle.PathHandle
    def o1: DataHandle.PathHandle
    def o2: DataHandle.PathHandle
    def nonExistentOutput: DataHandle.PathHandle
    
    final lazy val cachedOutput0: StoreRecord = o0.toStoreRecord
    final lazy val cachedOutput1: StoreRecord = o1.toStoreRecord
    final lazy val cachedOutput2: StoreRecord = o2.toStoreRecord
    final lazy val cachedNonExistentOutput: StoreRecord = nonExistentOutput.toStoreRecord
    
    final lazy val failedOutput0: StoreRecord = failedOutput(o0.path)
    final lazy val failedOutput1: StoreRecord = failedOutput(o1.path)
    final lazy val failedOutput2: StoreRecord = failedOutput(o2.path)
  }
  
  private object SimpleOutputs extends Outputs {
    override val p0 = path("src/test/resources/for-hashing/foo.txt")
    override val p1 = path("src/test/resources/for-hashing/empty.txt")
    override val p2 = path("src/test/resources/for-hashing/subdir/bar.txt")
    
    override val o0 = DataHandle.PathHandle(p0)
    override val o1 = DataHandle.PathHandle(p1)
    override val o2 = DataHandle.PathHandle(p2)
    override val nonExistentOutput = DataHandle.PathHandle(nonexistentPath)
  }
  
  private def testWithSimpleOutputSet(name: String)(body: Outputs => Any): Unit = {
    test(name) {
      body(SimpleOutputs)
    }
  }

  private val run: Run = Run.create()
  
  import loamstream.TestHelpers.dummyJobDir
  import loamstream.model.jobs.JobResult._

  test("sanity check paths") {
    import java.nio.file.Files.exists
    import SimpleOutputs._
    
    assert(exists(p0))
    assert(exists(p1))
    assert(exists(p2))
    assert(exists(nonexistentPath) === false)
    
    assert(o0.isMissing === false)
    assert(o1.isMissing === false)
    assert(o2.isMissing === false)
    assert(nonExistentOutput.isMissing)
    
    assert(cachedOutput0.isMissing === false)
    assert(cachedOutput1.isMissing === false)
    assert(cachedOutput2.isMissing === false)
    assert(cachedNonExistentOutput.isMissing)
  }
  
  testWithSimpleOutputSet("shouldRun - failed and successful runs") { outputs =>
    registerRunAndThen(run) {
      val filter = new DbBackedJobFilter(dao, HashingStrategy.HashOutputs)
      val recorder = new DbBackedExecutionRecorder(dao)
        
      val jobName = "dummyJob"
      
      assert(executions === Nil)

      val failedCommandLine = mockCmd
      val successfulCommandLine = s"${mockCmd}asdfasdf"
      
      def commandLineJob(commandLine: String, outputs: Set[DataHandle]) = {
        CommandLineJob(commandLine, TestHelpers.path("."), LocalSettings, outputs = outputs)
      }
      
      val successfulJob = commandLineJob(successfulCommandLine, Set(outputs.o0))
      val failedJob = commandLineJob(failedCommandLine, Set(outputs.o0))
      
      {
        val failure = CommandResult(42)
        assert(failure.isFailure)
  
        val success = CommandResult(0)
        assert(success.isSuccess)
        
        val failedExec = Execution.fromOutputs(
            mockUgerSettings, 
            failedCommandLine, 
            failure, 
            dummyJobDir, 
            failedJob.outputs)
        
        val successfulExec = Execution.fromOutputs(
            mockUgerSettings, 
            successfulCommandLine, 
            success, 
            dummyJobDir, 
            successfulJob.outputs)
  
        recorder.record(TestHelpers.DummyJobOracle, Seq(failedJob -> failedExec, successfulJob -> successfulExec))
      }
      
      //Doesn't need to be re-run
      assert(filter.shouldRun(successfulJob) === false)
      
      //Should run because a job with the same command line failed "last time"
      assert(filter.shouldRun(failedJob))
    }
  }
  
  testWithSimpleOutputSet("needsToBeRun/hasDifferentHash/hasDifferentModTime - should hash") { outputs =>
    registerRunAndThen(run) {
      val filter = new DbBackedJobFilter(dao, HashingStrategy.HashOutputs)
      val recorder = new DbBackedExecutionRecorder(dao)
      
      val jobName = "dummyJob"

      assert(executions === Nil)

      import outputs.{o0, o1, o2, nonExistentOutput, cachedNonExistentOutput, cachedOutput0, cachedOutput1 }
      
      {
        val failure = CommandResult(42)
        assert(failure.isFailure)
  
        val success = CommandResult(0)
        assert(success.isSuccess)
        
        val failedExec = Execution.fromOutputs(mockUgerSettings, mockCmd, failure, dummyJobDir, Set(o0))
        
        val successfulExec = {
          Execution.fromOutputs(mockUgerSettings, mockCmd, success, dummyJobDir, Set(o1, nonExistentOutput))
        }
  
        val executionTuples = {
          Seq(MockJob(failure.toJobStatus) -> failedExec, MockJob(success.toJobStatus) -> successfulExec)
        }
        
        recorder.record(TestHelpers.DummyJobOracle, executionTuples)
      }

      // Missing record:  'hasDifferentHash' --> false
      //                  'hasDifferentModTime --> false
      //                  'needsToBeRun' --> true
      assert(cachedNonExistentOutput.isMissing)
      assert(filter.hasDifferentHash(cachedNonExistentOutput) === false)
      assert(filter.hasDifferentModTime(cachedNonExistentOutput) === false)
      assert(filter.needsToBeRun(jobName, cachedNonExistentOutput))

      // Older record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'hasDifferentModTime --> true
      //                                                'needsToBeRun' --> true
      {
        val olderRec = cachedOutput1.withLastModified(Instant.ofEpochMilli(0))
        assert(filter.hasDifferentHash(olderRec) === false)
        assert(filter.hasDifferentModTime(olderRec))
        assert(filter.needsToBeRun(jobName, olderRec))
      }
      
      // Newer record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'hasDifferentModTime --> true
      //                                                'needsToBeRun' --> true
      {
        val newerRec = {
          val modTimePlusOne = Instant.ofEpochMilli(1 + cachedOutput1.lastModified.get.toEpochMilli)
          
          cachedOutput1.withLastModified(modTimePlusOne)
        }
        assert(filter.hasDifferentHash(newerRec) === false)
        assert(filter.hasDifferentModTime(newerRec))
        assert(filter.needsToBeRun(jobName, newerRec))
      }
      
      // Same record (as is matching record in DB): 'hasDifferentHash' --> false
      //                                            'hasDifferentModTime --> false
      //                                            'needsToBeRun' --> true
      assert(filter.hasDifferentHash(cachedOutput1) === false)
      assert(filter.hasDifferentModTime(cachedOutput1) === false)
      assert(filter.needsToBeRun(jobName, cachedOutput1) === false)

      // Unhashed record: 'needsToBeRun' --> true
      assert(filter.needsToBeRun(jobName, cachedOutput0))

      // Record with different hash:  'hasDifferentHash' --> true
      //                              'needsToBeRun' --> true
      val recWithDiffHash = StoreRecord(cachedOutput1.loc,
                                        () => Option("bogus-hash"),
                                        () => Option(Sha1.algorithmName),
                                        cachedOutput1.lastModified)
                                          
      assert(filter.hasDifferentHash(recWithDiffHash))
      assert(filter.needsToBeRun(jobName, recWithDiffHash))
      assert(filter.hasDifferentModTime(recWithDiffHash) === false)

      // Otherwise: 'needsToBeRun' --> false
      assert(filter.needsToBeRun(jobName, cachedOutput1) === false)
    }
  }
  
  testWithSimpleOutputSet("needsToBeRun/hasDifferentHash/hasDifferentModTime - hashing disabled") { outputs =>
    registerRunAndThen(run) {
      val filter = new DbBackedJobFilter(dao, HashingStrategy.DontHashOutputs)
      val recorder = new DbBackedExecutionRecorder(dao)
      val jobName = "dummyJob"

      assert(executions === Nil)

      import outputs.{o0, o1, o2, nonExistentOutput, cachedNonExistentOutput, cachedOutput0, cachedOutput1 }
      
      {
        val failure = CommandResult(42)
        assert(failure.isFailure)
  
        val success = CommandResult(0)
        assert(success.isSuccess)

        val failedExec = Execution.fromOutputs(mockUgerSettings, mockCmd, failure, dummyJobDir, Set(o0))
        
        val successfulExec = {
          Execution.fromOutputs(mockUgerSettings, mockCmd, success, dummyJobDir, Set(o1, nonExistentOutput))
        }
  
        val executionTuples = {
          Seq(MockJob(failure.toJobStatus) -> failedExec, MockJob(success.toJobStatus) -> successfulExec)
        }
        
        recorder.record(TestHelpers.DummyJobOracle, executionTuples)
      }

      // Missing record:  'hasDifferentHash' --> false
      //                  'hasDifferentModTime --> false
      //                  'needsToBeRun' --> true
      assert(cachedNonExistentOutput.isMissing)
      assert(filter.hasDifferentHash(cachedNonExistentOutput) === false)
      assert(filter.hasDifferentModTime(cachedNonExistentOutput) === false)
      assert(filter.needsToBeRun(jobName, cachedNonExistentOutput))

      // Older record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'hasDifferentModTime --> true
      //                                                'needsToBeRun' --> true
      {
        val olderRec = cachedOutput1.withLastModified(Instant.ofEpochMilli(0))

        assert(filter.hasDifferentHash(olderRec) === false)
        assert(filter.hasDifferentModTime(olderRec))
        assert(filter.needsToBeRun(jobName, olderRec))
      }
      
      // Newer record (than its matching record in DB): 'hasDifferentHash' --> false
      //                                                'hasDifferentModTime --> true
      //                                                'needsToBeRun' --> true
      {
        val newerRec = {
          val modTimePlusOne = Instant.ofEpochMilli(1 + cachedOutput1.lastModified.get.toEpochMilli)
          
          cachedOutput1.withLastModified(modTimePlusOne)
        }
        assert(filter.hasDifferentHash(newerRec) === false)
        assert(filter.hasDifferentModTime(newerRec))
        assert(filter.needsToBeRun(jobName, newerRec))
      }
      
      // Same record (as is matching record in DB): 'hasDifferentHash' --> false
      //                                            'hasDifferentModTime --> false
      //                                            'needsToBeRun' --> true
      assert(filter.hasDifferentHash(cachedOutput1) === false)
      assert(filter.hasDifferentModTime(cachedOutput1) === false)
      assert(filter.needsToBeRun(jobName, cachedOutput1) === false)

      // Unhashed record: 'needsToBeRun' --> false, since hashes aren't considered
      assert(filter.needsToBeRun(jobName, cachedOutput0) === false)

      // Record with different hash:  'hasDifferentHash' --> true
      //                              'needsToBeRun' --> true
      val recWithDiffHash = StoreRecord(cachedOutput1.loc,
                                        () => Option("bogus-hash"),
                                        () => Option(Sha1.algorithmName),
                                        cachedOutput1.lastModified)
      assert(filter.hasDifferentHash(recWithDiffHash))
      assert(filter.hasDifferentModTime(recWithDiffHash) === false)
      //since hashes aren't considered
      assert(filter.needsToBeRun(jobName, recWithDiffHash) === false)

      // Otherwise: 'needsToBeRun' --> false
      assert(filter.needsToBeRun(jobName, cachedOutput1) === false)
    }
  }

  testWithSimpleOutputSet("command string comparison") { outputSet =>
    def cmdLineJob(cmd: String, outputs: Set[DataHandle]): CommandLineJob = {
      CommandLineJob(
        commandLineString = cmd,
        workDir = Paths.getCurrentDirectory,
        initialSettings = mockUgerSettings,
        outputs = outputs)
    }

    def mockJob(cmd: String, outputs: Set[DataHandle]): MockJob = {
      new MockJob.FromJobFn(
        toReturnFn = mockRunData(_),
        name = "mock job",
        dependencies = Set.empty,
        successorsFn = () => Set.empty,
        inputs = Set.empty, //TODO
        outputs = outputs,
        delay = 0)
    }

    def execution(cmd: String, outputs: Set[DataHandle]): Execution = {
      Execution.fromOutputs(mockUgerSettings, cmd, CommandResult(0), dummyJobDir, outputs)
    }

    val cmd0 = "cmd0"
    val cmd1 = "cmd1"
    
    import outputSet.{o0, o1, o2 }
    
    registerRunAndThen(run) {
      val filter = new DbBackedJobFilter(dao)

      val outputs: Set[DataHandle] = Set(o0, o1)

      val recorder = new DbBackedExecutionRecorder(dao)

      val cmdLineJob0 = cmdLineJob(cmd0, outputs)
      
      recorder.record(TestHelpers.DummyJobOracle, Iterable(cmdLineJob0 -> execution(cmd0, outputs)))

      assert(filter.findCommandLineInDb(o0.location) === Some(cmd0))
      assert(filter.findCommandLineInDb(o1.location) === Some(cmd0))

      assert(filter.hasNewCommandLine(cmdLineJob0) === false)

      recorder.record(
          TestHelpers.DummyJobOracle, 
          Iterable(cmdLineJob(cmd1, Set(o2)) -> execution(cmd1, Set[DataHandle](o2))))
      
      val cmdLineJob1 = cmdLineJob("cmd1-altered", Set(o2))

      assert(filter.hasNewCommandLine(cmdLineJob1) === true)

      // Non-CommandLineJob's shouldn't be affected by cmd string checks
      val nonCmdLineJob0 = mockJob(cmd0, outputs)

      assert(filter.hasNewCommandLine(nonCmdLineJob0) === false)

      val nonCmdLineJob1 = mockJob("cmd1-altered", Set(o2))

      assert(filter.hasNewCommandLine(nonCmdLineJob1) === false)
    }
  }
}
