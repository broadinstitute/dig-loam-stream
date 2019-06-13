package loamstream.drm

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Try

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef
import loamstream.conf.ExecutionConfig
import loamstream.conf.LsfConfig
import loamstream.conf.UgerConfig
import loamstream.drm.DrmChunkRunnerTest.MockDrmJob
import loamstream.drm.DrmChunkRunnerTest.MockJobSubmitter
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.drm.uger.UgerDefaults
import loamstream.drm.uger.UgerPathBuilder
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamGraph
import loamstream.loam.LoamScriptContext
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.EnvironmentType
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LocalJob
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.RunData
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.commandline.HasCommandLine
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.Observables

import rx.lang.scala.Observable
import rx.lang.scala.schedulers.IOScheduler
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings


/**
 * @author clint
 * @author kyuksel
 * Jul 25, 2016
 */
final class DrmChunkRunnerTest extends FunSuite {
  
  private val scheduler = IOScheduler()
  
  import loamstream.TestHelpers.path
  import loamstream.TestHelpers.neverRestart
  import loamstream.model.jobs.JobStatus.FailedPermanently
  import loamstream.model.jobs.JobStatus.Succeeded
  
  private val tempDir = TestHelpers.getWorkDir(getClass.getSimpleName) 
  
  private val ugerConfig = {
    import scala.concurrent.duration.DurationInt
    
    UgerConfig(
      defaultCores = Cpus(2),
      defaultMemoryPerCore = Memory.inGb(2),
      defaultMaxRunTime = CpuTime.inHours(7))
  }
  
  private val lsfConfig = {
    import scala.concurrent.duration.DurationInt
    
    LsfConfig(
      maxNumJobs = 42,
      defaultCores = Cpus(2),
      defaultMemoryPerCore = Memory.inGb(2),
      defaultMaxRunTime = CpuTime.inHours(7))
  }
  
  private val executionConfig: ExecutionConfig = ExecutionConfig(42) 
  
  import loamstream.TestHelpers.waitFor
  import loamstream.util.Observables.Implicits._
  
  private object JustFailsMockPoller extends Poller {
    override def poll(jobIds: Iterable[String]): Map[String, Try[DrmStatus]] = ???
    
    override def stop(): Unit = ()
  }
  
  private val ugerPathBuilder = new UgerPathBuilder(UgerScriptBuilderParams(ugerConfig))
  
  test("No failures when empty set of jobs is presented - Uger") {
    val mockDrmClient = new MockDrmaaClient(Map.empty)
    val runner = DrmChunkRunner(
        environmentType = EnvironmentType.Uger,
        pathBuilder = ugerPathBuilder,
        executionConfig = executionConfig,
        drmConfig = ugerConfig,
        jobSubmitter = JobSubmitter.Drmaa(mockDrmClient, ugerConfig),
        //NB: The poller can always fail, since it should never be invoked
        jobMonitor = new JobMonitor(scheduler, JustFailsMockPoller),
        accountingClient = MockAccountingClient.NeverWorks)
    
    val result = waitFor(runner.run(Set.empty, TestHelpers.DummyJobOracle, neverRestart).firstAsFuture)
    
    assert(result === Map.empty)
  }
  
  test("No failures when empty set of jobs is presented - Lsf") {
    
    val runner = DrmChunkRunner(
        environmentType = EnvironmentType.Lsf,
        pathBuilder = LsfPathBuilder,
        executionConfig = executionConfig,
        drmConfig = lsfConfig,
        jobSubmitter = new MockJobSubmitter,
        //NB: The poller can always fail, since it should never be invoked
        jobMonitor = new JobMonitor(scheduler, JustFailsMockPoller),
        accountingClient = MockAccountingClient.NeverWorks)
    
    val result = waitFor(runner.run(Set.empty, TestHelpers.DummyJobOracle, neverRestart).firstAsFuture)
    
    assert(result === Map.empty)
  }
  
  test("combine") {
    import DrmChunkRunner.combine
    
    assert(combine(Map.empty, Map.empty) == Map.empty)
    
    val m1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    
    assert(combine(Map.empty, m1) == Map.empty)
    
    assert(combine(m1, Map.empty) == Map.empty)
    
    val m2 = Map("a" -> 42.0, "c" -> 99.0, "x" -> 123.456)
    
    assert(combine(m1, m2) == Map("a" -> (1, 42.0), "c" -> (3, 99.0)))
    
    assert(combine(m2, m1) == Map("a" -> (42.0, 1), "c" -> (99.0, 3)))
  }
  
  test("handleFailureStatus") {
    import DrmChunkRunner.handleFailureStatus
    import JobStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    
    def doTest(failureStatus: JobStatus): Unit = {
      val job = MockJob(NotStarted)
      
      assert(job.status === NotStarted)
      
      handleFailureStatus(alwaysRestart, failureStatus)(job)
      
      assert(job.status === failureStatus)
      
      handleFailureStatus(neverRestart, failureStatus)(job)
      
      assert(job.status === FailedPermanently)
    }
    
    doTest(Failed)
    doTest(FailedWithException)
    doTest(Terminated)
  }

  test("handleDrmStatus") {
    import DrmChunkRunner.handleDrmStatus
    import JobStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    
    def doTest(drmStatus: DrmStatus, isFailure: Boolean): Unit = {
      val jobStatus = DrmStatus.toJobStatus(drmStatus)
      
      {
        val job = MockJob(NotStarted)
        
        assert(job.status === NotStarted)
        
        handleDrmStatus(alwaysRestart, job)(drmStatus)
        
        assert(job.status === jobStatus)
      }
      
      {
        val job = MockJob(NotStarted)
        
        assert(job.status === NotStarted)
      
        handleDrmStatus(neverRestart, job)(drmStatus)
      
        val expected = if(isFailure) FailedPermanently else jobStatus
      
        assert(job.status === expected)
      }
    }
    
    doTest(DrmStatus.Failed, isFailure = true)
    doTest(DrmStatus.CommandResult(1), isFailure = true)
    doTest(DrmStatus.DoneUndetermined, isFailure = true)
    doTest(DrmStatus.Suspended, isFailure = true)
    
    doTest(DrmStatus.Done, isFailure = false)
    doTest(DrmStatus.Queued, isFailure = false)
    doTest(DrmStatus.QueuedHeld, isFailure = false)
    doTest(DrmStatus.Requeued, isFailure = false)
    doTest(DrmStatus.RequeuedHeld, isFailure = false)
    doTest(DrmStatus.Running, isFailure = false)
    doTest(DrmStatus.Undetermined, isFailure = false)
    doTest(DrmStatus.CommandResult(0), isFailure = false)
  }
  
  private def toTuple(jobWrapper: DrmJobWrapper): (DrmJobWrapper, Observable[DrmStatus]) = {
    
    val mockJob = jobWrapper.commandLineJob.asInstanceOf[MockDrmJob]
    
    jobWrapper -> Observable.from(mockJob.statusesToReturn)
  }
  
  test("toRunDatas - one failed job") {
    import DrmChunkRunner.toRunDatas
    import DrmStatus._
    import TestHelpers.{alwaysRestart, neverRestart, defaultUgerSettings}
    import DrmChunkRunnerTest.MockDrmJob
    import Observables.Implicits._
    
    val id = "failed"
    
    def doTest(shouldRestart: LJob => Boolean, lastUgerStatus: DrmStatus, expectedLastStatus: JobStatus): Unit = {
      val job = MockDrmJob(id, Queued, Queued, Running, Running, lastUgerStatus)
      
      val failed = DrmJobWrapper(ExecutionConfig.default, defaultUgerSettings, ugerPathBuilder, job, path("."), 1)
      
      assert(job.runCount === 0)
      
      val accountingClient = MockAccountingClient.NeverWorks
      
      val result = waitFor(toRunDatas(accountingClient, shouldRestart, Map(id -> toTuple(failed))).firstAsFuture)
      
      val Seq((actualJob, runData)) = result.toSeq
      
      assert(actualJob === job)    
      assert(runData.jobStatus === JobStatus.Failed)
      //TODO: Other assertions about RunData?
      
      val expectedStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastStatus)
      
      val statuses = if(shouldRestart(job)) job.statuses.take(3) else job.statuses 

      assert(waitFor(statuses.to[Seq].firstAsFuture) === expectedStatuses)
      
      assert(job.runCount === 1)
    }
    
    doTest(neverRestart, Failed, JobStatus.FailedPermanently)
    doTest(neverRestart, DoneUndetermined, JobStatus.FailedPermanently)
    doTest(neverRestart, CommandResult(1), JobStatus.FailedPermanently)
    
    doTest(alwaysRestart, Failed, JobStatus.Failed)
    doTest(alwaysRestart, DoneUndetermined, JobStatus.Failed)
    doTest(alwaysRestart, CommandResult(1), JobStatus.Failed)
  }
  
  test("toRunDatas - one successful job") {
    import DrmChunkRunner.toRunDatas
    import DrmStatus._
    import TestHelpers.{alwaysRestart, neverRestart, defaultUgerSettings}
    import DrmChunkRunnerTest.MockDrmJob
    import Observables.Implicits._
    
    val id = "worked"
    
    def doTest(shouldRestart: LJob => Boolean, lastDrmStatus: DrmStatus, expectedLastStatus: JobStatus): Unit = {
      val job = MockDrmJob(id, Queued, Queued, Running, Running, lastDrmStatus)
      
      val accountingClient = MockAccountingClient.NeverWorks
      
      val worked = DrmJobWrapper(ExecutionConfig.default, defaultUgerSettings, ugerPathBuilder, job, path("."), 1)
      
      assert(job.runCount === 0)
      
      val result = waitFor(toRunDatas(accountingClient, shouldRestart, Map(id -> toTuple(worked))).firstAsFuture)
      
      val Seq((actualJob, runData)) = result.toSeq
      
      assert(actualJob === job)
      assert(runData.jobStatus === JobStatus.WaitingForOutputs)
      //TODO: Other assertions about execution?
      
      val expectedStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastStatus, FailedPermanently)

      //Transition to FailedPermanently to make sure the Observable job.status finishes one way or another, in the
      //case that the mock job "ends" with a non-terminal status like WaitingForOutputs.
      
      job.transitionTo(FailedPermanently)
      
      val statuses = job.statuses.to[Seq] 

      assert(waitFor(statuses.firstAsFuture) === expectedStatuses.distinct)
      
      assert(job.runCount === 1)
    }
    
    doTest(neverRestart, Done, JobStatus.WaitingForOutputs)
    doTest(neverRestart, CommandResult(0), JobStatus.WaitingForOutputs)
    
    doTest(alwaysRestart, Done, JobStatus.WaitingForOutputs)
    doTest(alwaysRestart, CommandResult(0), JobStatus.WaitingForOutputs)
  }
  
  test("toRunDatas - one successful job, one failed job") {
    import DrmChunkRunner.toRunDatas
    import DrmStatus._
    import TestHelpers.{alwaysRestart, neverRestart, defaultUgerSettings}
    import DrmChunkRunnerTest.MockDrmJob
    import Observables.Implicits._
    
    val goodId = "worked"
    val badId = "failed"
    
    def doTest(
        shouldRestart: LJob => Boolean, 
        lastGoodDrmStatus: DrmStatus, 
        lastBadDrmStatus: DrmStatus, 
        expectedLastGoodStatus: JobStatus,
        expectedLastBadStatus: JobStatus): Unit = {
      
      val workedJob = MockDrmJob(goodId, Queued, Queued, Running, Running, lastGoodDrmStatus)
      val failedJob = MockDrmJob(badId, Queued, Queued, Running, Running, lastBadDrmStatus)
      
      val worked = DrmJobWrapper(ExecutionConfig.default, defaultUgerSettings, ugerPathBuilder, workedJob, path("."), 1)
      val failed = DrmJobWrapper(ExecutionConfig.default, defaultUgerSettings, ugerPathBuilder, failedJob, path("."), 2)
      
      assert(workedJob.runCount === 0)
      assert(failedJob.runCount === 0)
      
      val input = Map(goodId -> toTuple(worked), badId -> toTuple(failed))
      
      val accountingClient = MockAccountingClient.NeverWorks
      
      val result = waitFor(toRunDatas(accountingClient, shouldRestart, input).firstAsFuture)
      
      val goodExecution = result(workedJob)
      val badExecution = result(failedJob)
      
      assert(result.size === 2)
      
      assert(goodExecution.jobStatus === JobStatus.WaitingForOutputs)
      assert(badExecution.jobStatus === JobStatus.Failed)
      //TODO: Other assertions about execution?
      
      val expectedGoodStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastGoodStatus, Succeeded)
      val expectedBadStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastBadStatus)
      
      //Transition to Succeeded to make sure the Observable workedJob.status finishes one way or another, in the
      //case that that job "ends" with a non-terminal status like WaitingForOutputs.
      workedJob.transitionTo(Succeeded)
      
      val goodStatuses = workedJob.statuses.to[Seq] 
      val badStatuses = (if(shouldRestart(failedJob)) failedJob.statuses.take(3) else failedJob.statuses).to[Seq]

      assert(waitFor(goodStatuses.firstAsFuture) === expectedGoodStatuses.distinct)
      assert(waitFor(badStatuses.firstAsFuture) === expectedBadStatuses)
      
      assert(workedJob.runCount === 1)
      assert(failedJob.runCount === 1)
    }
    
    val failedPermanently = JobStatus.FailedPermanently
    
    doTest(neverRestart, Done, Failed, JobStatus.WaitingForOutputs, failedPermanently)
    
    doTest(
        neverRestart, 
        CommandResult(0), 
        CommandResult(1), 
        JobStatus.WaitingForOutputs, 
        failedPermanently)
    
    doTest(alwaysRestart, Done, Failed, JobStatus.WaitingForOutputs, JobStatus.Failed)
    
    doTest(
        alwaysRestart, 
        CommandResult(0), 
        CommandResult(1), 
        JobStatus.WaitingForOutputs, 
        JobStatus.Failed)
  }
  
  test("DRM config is propagated to DRMAA client - 2 jobs, same settings") {
    
    def makeGraph(drmSystem: DrmSystem): LoamGraph = {
      TestHelpers.makeGraph(drmSystem) { implicit context =>
        import LoamPredef._
        import LoamCmdTool._
      
        val a = store("a.txt").asInput
        val b = store("b.txt")
        val c = store("c.txt")
      
        drmWith(cores = 4, mem = 16, maxRunTime = 5) {
          cmd"cp $a $b".in(a).out(b)
          cmd"cp $a $c".in(a).out(c)
        }
      }
    }
    
    def makeChunkRunner(drmSystem: DrmSystem, mockJobSubmitter: MockJobSubmitter): DrmChunkRunner = drmSystem match {
      case DrmSystem.Uger => {
        DrmChunkRunner(
            environmentType = EnvironmentType.Uger,
            pathBuilder = ugerPathBuilder,
            executionConfig = executionConfig,
            drmConfig = ugerConfig,
            jobSubmitter = mockJobSubmitter,
            jobMonitor = new JobMonitor(poller = JustFailsMockPoller),
            accountingClient = MockAccountingClient.NeverWorks)
      }
      case DrmSystem.Lsf => {
        DrmChunkRunner(
            environmentType = EnvironmentType.Lsf,
            pathBuilder = LsfPathBuilder,
            executionConfig = executionConfig,
            drmConfig = lsfConfig,
            jobSubmitter = mockJobSubmitter,
            jobMonitor = new JobMonitor(poller = JustFailsMockPoller),
            accountingClient = MockAccountingClient.NeverWorks)
      }
    }
    
    def doTest(drmSystem: DrmSystem): Unit = {
      val graph = makeGraph(drmSystem)
      val executable = LoamEngine.toExecutable(graph)
      val jobs = executable.jobs.toSeq
      
      assert(jobs.size === 2)
      
      val queueOpt: Option[Queue] = drmSystem match {
        case DrmSystem.Uger => Option(UgerDefaults.queue)
        case DrmSystem.Lsf => None
      }
      
      val expectedSettings = drmSystem.settingsMaker(Cpus(4), Memory.inGb(16), CpuTime.inHours(5), queueOpt, None)
      
      assert(jobs(0).job.initialSettings === expectedSettings)
      assert(jobs(1).job.initialSettings === expectedSettings)
      
      val mockJobSubmitter = new MockJobSubmitter
      
      val chunkRunner = makeChunkRunner(drmSystem, mockJobSubmitter)
          
      
      val results = {
        waitFor(chunkRunner.run(jobs.map(_.job).toSet, TestHelpers.DummyJobOracle, neverRestart).firstAsFuture)
      }
      
      val actualSubmissionParams = mockJobSubmitter.params
      
      val Seq((actualSettings, actualSubmittedJobs)) = actualSubmissionParams
      
      assert(actualSettings === expectedSettings)
      assert(actualSubmittedJobs.drmJobs.map(_.commandLineJob).toSet === jobs.toSet)
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("Uger config is propagated to DRMAA client - 2 pairs of jobs with different settings") {
    
    def makeGraphAndTools(drmSystem: DrmSystem): (LoamGraph, LoamCmdTool, LoamCmdTool, LoamCmdTool, LoamCmdTool) = {
      implicit val sc = new LoamScriptContext(TestHelpers.emptyProjectContext(drmSystem))
        
      import loamstream.compiler.LoamPredef._
      import loamstream.loam.LoamCmdTool._
    
      val a = store("a.txt").asInput
      val b = store("b.txt")
      val c = store("c.txt")
      val d = store("d.txt")
      val e = store("e.txt")
    
      val (tool0, tool1) = drmWith(cores = 4, mem = 16, maxRunTime = 5) {
        (cmd"cp $a $b".in(a).out(b)) -> (cmd"cp $a $c".in(a).out(c))
      }
      
      val (tool2, tool3) = drmWith(cores = 7, mem = 9, maxRunTime = 11) {
        (cmd"cp $a $d".in(a).out(d)) -> (cmd"cp $a $e".in(a).out(e))
      }
      
      (sc.projectContext.graph, tool0, tool1, tool2, tool3)
    }
    
    def makeChunkRunner(drmSystem: DrmSystem, mockJobSubmitter: MockJobSubmitter): DrmChunkRunner = drmSystem match {
      case DrmSystem.Uger => { 
        val mockDrmaaClient = MockDrmaaClient(Map.empty)

        DrmChunkRunner(
            environmentType = EnvironmentType.Uger,
            pathBuilder = ugerPathBuilder,
            executionConfig = executionConfig,
            drmConfig = ugerConfig,
            jobSubmitter = mockJobSubmitter,
            jobMonitor = new JobMonitor(poller = new DrmaaPoller(mockDrmaaClient)),
            accountingClient = MockAccountingClient.NeverWorks)
      }
      case DrmSystem.Lsf => {
        DrmChunkRunner(
            environmentType = EnvironmentType.Lsf,
            pathBuilder = LsfPathBuilder,
            executionConfig = executionConfig,
            drmConfig = lsfConfig,
            jobSubmitter = mockJobSubmitter,
            //NB: The poller can fail, since we're not checking execution results, just config-propagation
            jobMonitor = new JobMonitor(poller = JustFailsMockPoller),
            accountingClient = MockAccountingClient.NeverWorks)
      }
    }
    
    def doTest(drmSystem: DrmSystem): Unit = {
     
      val (graph, tool0, tool1, tool2, tool3) = makeGraphAndTools(drmSystem)
      val executable = LoamEngine.toExecutable(graph)
      val jobs = executable.jobs.toSeq
      
      assert(jobs.size === 4)
      
      val queueOpt: Option[Queue] = drmSystem match {
        case DrmSystem.Uger => Option(UgerDefaults.queue)
        case DrmSystem.Lsf => None
      }
      
      val expectedSettings0 = drmSystem.settingsMaker(Cpus(4), Memory.inGb(16), CpuTime.inHours(5), queueOpt, None)
      val expectedSettings1 = drmSystem.settingsMaker(Cpus(7), Memory.inGb(9), CpuTime.inHours(11), queueOpt, None)
      
      def findJob(tool: LoamCmdTool): CommandLineJob = {
        jobs.iterator.map(_.asInstanceOf[CommandLineJob]).find(_.commandLineString == tool.commandLine).get
      }
      
      assert(findJob(tool0).initialSettings === expectedSettings0)
      assert(findJob(tool1).initialSettings === expectedSettings0)
      
      assert(findJob(tool2).initialSettings === expectedSettings1)
      assert(findJob(tool3).initialSettings === expectedSettings1)
      
      val mockJobSubmitter = new MockJobSubmitter
      
      val chunkRunner = makeChunkRunner(drmSystem, mockJobSubmitter)
          
      val results = {
        waitFor(chunkRunner.run(jobs.map(_.job).toSet, TestHelpers.DummyJobOracle, neverRestart).firstAsFuture)
      }
      
      val actualSubmissionParams = mockJobSubmitter.params
      
      val actualParamsUnordered: Set[(DrmSettings, Set[LJob])] = {
        actualSubmissionParams.map { case (settings, taskArray) => 
          (settings, taskArray.drmJobs.map(_.commandLineJob).toSet[LJob]) 
        }.toSet
      }
      
      val expectedParamsUnordered: Set[(DrmSettings, Set[LJob])] = Set(
          expectedSettings0 -> Set(findJob(tool0), findJob(tool1)),
          expectedSettings1 -> Set(findJob(tool2), findJob(tool3)))
      
      assert(actualParamsUnordered === expectedParamsUnordered)
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("updateJobStatusesOnSubmission - SubmissionSuccess") {
    val foo = MockDrmJob("Foo", DrmStatus.Done)
    val bar = MockDrmJob("Bar", DrmStatus.CommandResult(42))
    val baz = MockDrmJob("Baz", DrmStatus.Done)
    
    val jobs: Seq[HasCommandLine] = Seq(foo, bar, baz)
    
    def wrap(job: HasCommandLine, index: Int): DrmJobWrapper = DrmJobWrapper(
        ExecutionConfig.default,
        //Use LSF params since they're easier to instantiate here, and LSF vs Uger doesn't matter in this case.
        DrmSettings.fromLsfConfig(LsfConfig()),
        LsfPathBuilder,
        job,
        path("."), 
        index)
    
    val submissionSuccess = DrmSubmissionResult.SubmissionSuccess(Map(
        "foo-id" -> wrap(foo, 1),
        "bar-id" -> wrap(bar, 2),
        "baz-id" -> wrap(baz, 3)))
        
    import JobStatus.NotStarted
    import JobStatus.Running

    assert(foo.status === NotStarted)
    assert(bar.status === NotStarted)
    assert(baz.status === NotStarted)
        
    DrmChunkRunner.updateJobStatusesOnSubmission(jobs, submissionSuccess, _ => ???)
    
    assert(foo.status === Running)
    assert(bar.status === Running)
    assert(baz.status === Running)
  }
}

object DrmChunkRunnerTest {
  final class MockJobSubmitter extends JobSubmitter {
    @volatile var params: Seq[(DrmSettings, DrmTaskArray)] = Vector.empty
    
    override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): DrmSubmissionResult = {
      params :+= (drmSettings -> taskArray)
      
      DrmSubmissionResult.SubmissionSuccess(Map.empty)
    }
    
    override def stop(): Unit = ()
  }
  
  final case class MockDrmJob(name: String, statusesToReturn: DrmStatus*) extends LocalJob with HasCommandLine {
    require(statusesToReturn.nonEmpty)

    override def commandLineString: String = name //NB: The content of the command line is unimportant
    
    override val initialSettings: Settings = LocalSettings
    
    override def dependencies: Set[JobNode] = Set.empty

    override def inputs: Set[DataHandle] = Set.empty
    
    override def outputs: Set[DataHandle] = Set.empty
    
    override def execute(implicit context: ExecutionContext): Future[RunData] = {
      val runData = RunData(
          job = this, 
          settings = LocalSettings,
          jobStatus = DrmStatus.toJobStatus(statusesToReturn.last), 
          jobResult = None, 
          resourcesOpt = None, 
          jobDirOpt = None,
          terminationReasonOpt = None)
      
      Future.successful(runData)
    }
  }
}
