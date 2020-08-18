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
import loamstream.util.ValueBox
import loamstream.drm.uger.MockQacctAccountingClient
import loamstream.drm.uger.QacctTestHelpers
import java.time.LocalDateTime
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.TerminationReason
import loamstream.model.execute.Resources.UgerResources
import loamstream.drm.uger.QdelJobKiller


/**
 * @author clint
 * @author kyuksel
 * Jul 25, 2016
 */
final class DrmChunkRunnerTest extends FunSuite {
  
  private val scheduler = IOScheduler()
  
  import loamstream.TestHelpers.path
  import loamstream.model.jobs.JobStatus.FailedPermanently
  import loamstream.model.jobs.JobStatus.Succeeded
  import scala.concurrent.ExecutionContext.Implicits.global
  
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
      maxNumJobsPerTaskArray = 42,
      defaultCores = Cpus(2),
      defaultMemoryPerCore = Memory.inGb(2),
      defaultMaxRunTime = CpuTime.inHours(7))
  }
  
  private val executionConfig: ExecutionConfig = ExecutionConfig(42) 
  
  import loamstream.TestHelpers.waitFor
  import loamstream.util.Observables.Implicits._
  
  private object JustFailsMockPoller extends Poller {
    override def poll(jobIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = ???
    
    override def stop(): Unit = ()
  }
  
  private val ugerPathBuilder = new UgerPathBuilder(UgerScriptBuilderParams(ugerConfig))
  
  test("No failures when empty set of jobs is presented - Uger") {
    val mockDrmClient = new MockPoller(Map.empty)
    val runner = DrmChunkRunner(
        environmentType = EnvironmentType.Uger,
        pathBuilder = ugerPathBuilder,
        executionConfig = executionConfig,
        drmConfig = ugerConfig,
        jobSubmitter = new MockJobSubmitter,
        //NB: The poller can always fail, since it should never be invoked
        jobMonitor = new JobMonitor(scheduler, JustFailsMockPoller),
        accountingClient = MockAccountingClient.NeverWorks,
        jobKiller = MockJobKiller.DoesNothing)
    
    val result = waitFor(runner.run(Set.empty, TestHelpers.DummyJobOracle).to[Seq].firstAsFuture)
    
    assert(result === Nil)
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
        accountingClient = MockAccountingClient.NeverWorks,
        jobKiller = MockJobKiller.DoesNothing)
    
    val result = waitFor(runner.run(Set.empty, TestHelpers.DummyJobOracle).to[Seq].firstAsFuture)
    
    assert(result === Nil)
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
  
  private def toTupleObs(jobWrapper: DrmJobWrapper): Observable[(DrmJobWrapper, DrmStatus)] = {
    
    val mockJob = jobWrapper.commandLineJob.asInstanceOf[MockDrmJob]
    
    Observable.from(mockJob.statusesToReturn).map(status => jobWrapper -> status)
  }
  
  /*private def toTuple(jobWrapper: DrmJobWrapper): (DrmJobWrapper, Observable[DrmStatus]) = {
    
    val mockJob = jobWrapper.commandLineJob.asInstanceOf[MockDrmJob]
    
    jobWrapper -> Observable.from(mockJob.statusesToReturn)
  }*/
  
  test("toRunDatas - one failed job") {
    import DrmChunkRunner.toRunDatas
    import DrmStatus._
    import TestHelpers.defaultUgerSettings
    import DrmChunkRunnerTest.MockDrmJob
    import Observables.Implicits._
    
    val id = DrmTaskId("failed", 1)
    
    def doTest(lastUgerStatus: DrmStatus, expectedLastStatus: JobStatus): Unit = {
      val job = MockDrmJob(id, Queued, Queued, Running, Running, lastUgerStatus)
      
      val failed = DrmJobWrapper(ExecutionConfig.default, defaultUgerSettings, ugerPathBuilder, job, path("."), 1)
      
      val accountingClient = new DrmChunkRunnerTest.MockAccountingClient(
          Map.empty[DrmTaskId, AccountingInfo].withDefault(_ => bogusAccountingInfo))
      
      val jobsAndDrmStatusesById = toTupleObs(failed).map { case (wrapper, status) => (id, wrapper, status) }
      
      val result = waitFor(toRunDatas(accountingClient, jobsAndDrmStatusesById).firstAsFuture)
      
      val (actualJob, runData) = result
      
      assert(actualJob === job)    
      assert(runData.jobStatus === JobStatus.Failed)
      assert(runData.resourcesOpt.isDefined)
      //TODO: Other assertions about RunData?
      
      val expectedStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastStatus)
      
      assert(runData.jobStatus === expectedStatuses.last)
    }
    
    doTest(Failed, JobStatus.Failed)
    doTest(DoneUndetermined, JobStatus.Failed)
    doTest(CommandResult(1), JobStatus.Failed)
    
    doTest(Failed, JobStatus.Failed)
    doTest(DoneUndetermined, JobStatus.Failed)
    doTest(CommandResult(1), JobStatus.Failed)
  }
  
  test("toRunDatas - one successful job") {
    import DrmChunkRunner.toRunDatas
    import DrmStatus._
    import TestHelpers.defaultUgerSettings
    import DrmChunkRunnerTest.MockDrmJob
    import Observables.Implicits._
    
    val id = DrmTaskId("worked", 1)
    
    def doTest(lastDrmStatus: DrmStatus, expectedLastStatus: JobStatus): Unit = {
      val job = MockDrmJob(id, Queued, Queued, Running, Running, lastDrmStatus)
      
      val accountingClient = new DrmChunkRunnerTest.MockAccountingClient(
          Map.empty[DrmTaskId, AccountingInfo].withDefault(_ => bogusAccountingInfo))
      
      val worked = DrmJobWrapper(ExecutionConfig.default, defaultUgerSettings, ugerPathBuilder, job, path("."), 1)
      
      val jobsAndDrmStatusesById = toTupleObs(worked).map { case (wrapper, status) => (id, wrapper, status) }
      
      val result = waitFor(toRunDatas(accountingClient, jobsAndDrmStatusesById).firstAsFuture)
      
      val (actualJob, runData) = result
      
      assert(actualJob === job)
      assert(runData.jobStatus === JobStatus.WaitingForOutputs)
      assert(runData.resourcesOpt === None)
      assert(runData.terminationReasonOpt === None)
    }
    
    doTest(Done, JobStatus.WaitingForOutputs)
    doTest(CommandResult(0), JobStatus.WaitingForOutputs)
    
    doTest(Done, JobStatus.WaitingForOutputs)
    doTest(CommandResult(0), JobStatus.WaitingForOutputs)
  }
  
  test("toRunDatas - one successful job, one failed job") {
    import DrmChunkRunner.toRunDatas
    import DrmStatus._
    import TestHelpers.defaultUgerSettings
    import DrmChunkRunnerTest.MockDrmJob
    import Observables.Implicits._
    
    val goodId = DrmTaskId("worked", 1)
    val badId = DrmTaskId("failed", 2)
    
    def doTest(
        lastGoodDrmStatus: DrmStatus, 
        lastBadDrmStatus: DrmStatus, 
        expectedLastGoodStatus: JobStatus,
        expectedLastBadStatus: JobStatus): Unit = {
      
      val workedJob = MockDrmJob(goodId, Queued, Queued, Running, Running, lastGoodDrmStatus)
      val failedJob = MockDrmJob(badId, Queued, Queued, Running, Running, lastBadDrmStatus)
      
      val worked = DrmJobWrapper(ExecutionConfig.default, defaultUgerSettings, ugerPathBuilder, workedJob, path("."), 1)
      val failed = DrmJobWrapper(ExecutionConfig.default, defaultUgerSettings, ugerPathBuilder, failedJob, path("."), 2)
      
      val forGoodId = toTupleObs(worked).map { case (wrapper, status) => (goodId, wrapper, status) }
      val forBadId = toTupleObs(failed).map { case (wrapper, status) => (badId, wrapper, status) }
      
      val input = Observables.merge(Seq(forGoodId, forBadId))
      
      import QacctTestHelpers.{successfulRun, actualQacctOutput}

      val accountingClient = new DrmChunkRunnerTest.MockAccountingClient(
          Map.empty[DrmTaskId, AccountingInfo].withDefault(_ => bogusAccountingInfo))
      
      val result = waitFor(toRunDatas(accountingClient, input).to[Seq].map(_.toMap).firstAsFuture)
      
      val goodExecution = result(workedJob)
      val badExecution = result(failedJob)
      
      assert(result.size === 2)
      
      assert(goodExecution.jobStatus === JobStatus.WaitingForOutputs)
      assert(badExecution.jobStatus === JobStatus.Failed)
      
      assert(goodExecution.resourcesOpt === None)
      assert(badExecution.resourcesOpt.isDefined)
      //TODO: Other assertions about execution?
      
      val expectedGoodStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastGoodStatus, Succeeded)
      val expectedBadStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastBadStatus)
      
      assert(goodExecution.jobStatus === expectedLastGoodStatus)
      assert(badExecution.jobStatus === expectedLastBadStatus)
    }
    
    val failedPermanently = JobStatus.FailedPermanently
    
    doTest(Done, Failed, JobStatus.WaitingForOutputs, JobStatus.Failed)
    
    doTest(
        CommandResult(0), 
        CommandResult(1), 
        JobStatus.WaitingForOutputs, 
        JobStatus.Failed)
    
    doTest(Done, Failed, JobStatus.WaitingForOutputs, JobStatus.Failed)
    
    doTest(
        CommandResult(0), 
        CommandResult(1), 
        JobStatus.WaitingForOutputs, 
        JobStatus.Failed)
  }
  
  test("DRM config is propagated to JobSubmitter - 2 jobs, same settings") {
    
    def makeGraph(drmSystem: DrmSystem): LoamGraph = {
      TestHelpers.makeGraph(drmSystem) { implicit context =>
        import loamstream.loam.LoamSyntax._
      
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
            accountingClient = MockAccountingClient.NeverWorks,
            jobKiller = MockJobKiller.DoesNothing)
      }
      case DrmSystem.Lsf => {
        DrmChunkRunner(
            environmentType = EnvironmentType.Lsf,
            pathBuilder = LsfPathBuilder,
            executionConfig = executionConfig,
            drmConfig = lsfConfig,
            jobSubmitter = mockJobSubmitter,
            jobMonitor = new JobMonitor(poller = JustFailsMockPoller),
            accountingClient = MockAccountingClient.NeverWorks,
            jobKiller = MockJobKiller.DoesNothing)
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
        waitFor(chunkRunner.run(jobs.map(_.job).toSet, TestHelpers.DummyJobOracle).to[Seq].map(_.toMap).firstAsFuture)
      }
      
      val actualSubmissionParams = mockJobSubmitter.params
      
      val Seq((actualSettings, actualSubmittedJobs)) = actualSubmissionParams
      
      assert(actualSettings === expectedSettings)
      assert(actualSubmittedJobs.drmJobs.map(_.commandLineJob).toSet === jobs.toSet)
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("Uger config is propagated to JobSubmitter - 2 pairs of jobs with different settings") {
    
    def makeGraphAndTools(drmSystem: DrmSystem): (LoamGraph, LoamCmdTool, LoamCmdTool, LoamCmdTool, LoamCmdTool) = {
      implicit val sc = new LoamScriptContext(TestHelpers.emptyProjectContext(drmSystem))
        
      import loamstream.loam.LoamSyntax._
    
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
        DrmChunkRunner(
            environmentType = EnvironmentType.Uger,
            pathBuilder = ugerPathBuilder,
            executionConfig = executionConfig,
            drmConfig = ugerConfig,
            jobSubmitter = mockJobSubmitter,
            jobMonitor = new JobMonitor(poller = MockPoller(Map.empty)),
            accountingClient = MockAccountingClient.NeverWorks,
            jobKiller = MockJobKiller.DoesNothing)
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
            accountingClient = MockAccountingClient.NeverWorks,
            jobKiller = MockJobKiller.DoesNothing)
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
        waitFor(chunkRunner.run(jobs.map(_.job).toSet, TestHelpers.DummyJobOracle).to[Seq].map(_.toMap).firstAsFuture)
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
  
  test("notSuccess") {
    import DrmChunkRunner.notSuccess
    
    assert(notSuccess(DrmStatus.Done) === false)
    assert(notSuccess(DrmStatus.CommandResult(0)) === false)
    assert(notSuccess(DrmStatus.CommandResult(42)) === true)
    assert(notSuccess(DrmStatus.DoneUndetermined) === true)
    assert(notSuccess(DrmStatus.Failed) === true)
    assert(notSuccess(DrmStatus.Queued) === true)
    assert(notSuccess(DrmStatus.QueuedHeld) === true)
    assert(notSuccess(DrmStatus.Requeued) === true)
    assert(notSuccess(DrmStatus.RequeuedHeld) === true)
    assert(notSuccess(DrmStatus.Running) === true)
    assert(notSuccess(DrmStatus.Suspended) === true)
    assert(notSuccess(DrmStatus.Undetermined) === true)
  }
  
  private def bogusAccountingInfo: AccountingInfo = AccountingInfo(
    UgerResources(
      Memory.inGb(1),
      CpuTime.inSeconds(1),
      None,
      None,
      LocalDateTime.now,
      LocalDateTime.now), 
    None)
}

object DrmChunkRunnerTest {
  final class MockJobSubmitter extends JobSubmitter {
    @volatile var params: Seq[(DrmSettings, DrmTaskArray)] = Vector.empty
    
    override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
      params :+= (drmSettings -> taskArray)
      
      Observable.just(DrmSubmissionResult.SubmissionSuccess(Map.empty))
    }
    
    override def stop(): Unit = ()
  }
  
  final case class MockDrmJob(taskId: DrmTaskId, statusesToReturn: DrmStatus*) extends LocalJob with HasCommandLine {
    
    require(statusesToReturn.nonEmpty)

    override def name: String = s"${taskId.jobId}[${taskId.taskIndex}]"
    
    override def commandLineString: String = name //NB: The content of the command line is unimportant
    
    override val initialSettings: Settings = LocalSettings
    
    override def dependencies: Set[JobNode] = Set.empty
    
    override def successors: Set[JobNode] = Set.empty

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
  
  final class MockAccountingClient(toReturn: Map[DrmTaskId, AccountingInfo]) extends AccountingClient {
    override def getResourceUsage(taskId: DrmTaskId): Future[DrmResources] = Future.successful {
      toReturn(taskId).resources
    }
  
    override def getTerminationReason(taskId: DrmTaskId): Future[Option[TerminationReason]] = Future.successful {
      toReturn.get(taskId).flatMap(_.terminationReasonOpt)
    }
  }
}
