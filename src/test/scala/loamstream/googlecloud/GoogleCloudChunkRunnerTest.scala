package loamstream.googlecloud

import java.nio.file.Path

import scala.concurrent.ExecutionContext

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.MockChunkRunner
import loamstream.model.execute.ProvidesEnvAndResources
import loamstream.model.execute.Resources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.LJob
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.RunData
import loamstream.util.ValueBox

import scala.collection.compat._
import loamstream.util.Maps
import loamstream.util.Observables


/**
 * @author clint
 * Dec 15, 2016
 */
final class GoogleCloudChunkRunnerTest extends FunSuite with ProvidesEnvAndResources {
  
  import GoogleCloudChunkRunnerTest.LiteralMockDataProcClient
  import GoogleCloudChunkRunnerTest.MockDataProcClient
  import loamstream.TestHelpers.waitForT
  import loamstream.util.Observables.Implicits._
  import monix.execution.Scheduler.Implicits.global

  private val clusterId = "some-cluster-id"
  
  private val googleConfig = {
    import TestHelpers.path
    
    GoogleCloudConfig(path("gcloud"), path("gsutil"), "some-project-id", clusterId, path("creds-file"), "some-region")
  }

  private val clusterConfig0 = ClusterConfig.default.copy(zone = "foo")
  private val clusterConfig1 = clusterConfig0.copy(numWorkers = 99)
  private val clusterConfig2 = clusterConfig0.copy(numWorkers = 42)
    
  assert(clusterConfig0 !== clusterConfig1)
  assert(clusterConfig0 !== clusterConfig2)
  assert(clusterConfig1 !== clusterConfig2)
  
  private def clusterConfigFrom(j: LJob): ClusterConfig = j.initialSettings.asInstanceOf[GoogleSettings].clusterConfig
  
  private def mockJob(
      result: JobResult, 
      resources: Option[Resources] = None, 
      jobDir: Option[Path] = None,
      initialSettings: GoogleSettings = GoogleSettings(clusterId, clusterConfig0)) = {
    
    MockJob(result, resources, jobDir, initialSettings = initialSettings)
  }

  test("addCluster") {
    import GoogleCloudChunkRunner.addCluster

    val clusterId = "foo"
    
    val localResources = TestHelpers.localResources
    
    val ugerResources = TestHelpers.ugerResources
    val googleResources = TestHelpers.googleResources
    
    val job1 = mockJob(CommandResult(0), Some(localResources))
    val job2 = mockJob(CommandResult(1), Some(ugerResources))
    val job3 = mockJob(CommandResult(2), Some(googleResources))
    
    val input: Map[LJob, RunData] = Map(
        job1 -> job1.toReturn, 
        job2 -> job2.toReturn, 
        job3 -> job3.toReturn)
    
    {
      val (_, runData1) = addCluster(clusterId)(job1 -> job1.toReturn)
    
      val job1Result = runData1.jobResult.get.asInstanceOf[CommandResult]
    
      assert(job1Result.exitCode === 0)
      assert(
          runData1.resourcesOpt.get === GoogleResources(clusterId, localResources.startTime, localResources.endTime))
    }
    
    {
      val (_, runData) = addCluster(clusterId)(job1 -> job2.toReturn)
      
      assert(runData === job2.toReturn)
    }
    
    {
      val (_, runData) = addCluster(clusterId)(job1 -> job3.toReturn)
      
      assert(runData === job3.toReturn)
    }
  }
  
  test("withCluster") {
    import Observables.Implicits._
    
    withMockRunner { (_, googleRunner, mockClient) =>
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(mockClient.clusterRunning === false)
      assert(mockClient.startClusterInvocations === Nil)
      assert(mockClient.deleteClusterInvocations === 0)

      googleRunner.withCluster(clusterConfig0) { 
        assert(mockClient.clusterRunning === true)
        assert(mockClient.startClusterInvocations === Seq(clusterConfig0))
        assert(mockClient.deleteClusterInvocations === 0)
      }
      
      googleRunner.withCluster(clusterConfig1) {
        assert(mockClient.clusterRunning === true)
        assert(mockClient.startClusterInvocations === Seq(clusterConfig0, clusterConfig1))
        assert(mockClient.deleteClusterInvocations === 1)
      }
      
      googleRunner.withCluster(clusterConfig0) {
        assert(mockClient.clusterRunning === true)
        assert(mockClient.startClusterInvocations === Seq(clusterConfig0, clusterConfig1, clusterConfig0))
        assert(mockClient.deleteClusterInvocations === 2)
      }
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(mockClient.clusterRunning === true)
      assert(mockClient.startClusterInvocations === Seq(clusterConfig0, clusterConfig1, clusterConfig0))
      assert(mockClient.deleteClusterInvocations === 2)
    } 
  }
  
  test("runJobsSequentially") {
    val localResources = TestHelpers.localResources
    
    val job1 = mockJob(JobResult.Success)
    val job2 = mockJob(JobResult.Failure)
    val job3 = mockJob(JobResult.CommandResult(0), Some(localResources))
    
    withMockRunner { (mockRunner, actualGoogleRunner, client) =>
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Nil)
      assert(client.deleteClusterInvocations === 0)
      
      val runDataObs = {
        actualGoogleRunner.runJobsSequentially(Set(job1, job2, job3), TestHelpers.DummyJobOracle)
      }
      
      val z: Map[LJob, RunData] = Map.empty 
      
      val runDatas = waitForT(runDataObs.foldLeft(z)(_ + _).firstL)
      
      assert(runDatas(job1) === job1.toReturn)
      assert(runDatas(job2) === job2.toReturn)
      
      val job3RunData = runDatas(job3)
      val job3Result = job3RunData.jobResult.get.asInstanceOf[JobResult.CommandResult]
      
      assert(job3Result.exitCode === 0)

      val job3Resources = job3RunData.resourcesOpt.get.asInstanceOf[GoogleResources]
      
      assert(job3Resources.clusterId === clusterId)
      assert(job3Resources.startTime === localResources.startTime)
      assert(job3Resources.endTime === localResources.endTime)
      
      assert(runDatas.size === 3)
    }
  }
  
  test("stop()") {
    withMockRunner { (_, googleRunner, mockClient) =>
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(mockClient.clusterRunning === false)
          
      googleRunner.withCluster(clusterConfig0)(42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(mockClient.clusterRunning === true)
      
      googleRunner.stop()
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
      assert(mockClient.clusterRunning === false)
    }
  }
  
  test("stop() - error checking cluster status") {
    withMockClient { delegateClient =>
      
      val client = new LiteralMockDataProcClient(delegateClient, ???)
      
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning === false)
          
      googleRunner.withCluster(clusterConfig0)(42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning === true)
      
      //Make sure the cluster is still shut down, even if the client throws when checking whether the
      //cluster is running.
      
      googleRunner.stop()
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
      assert(client.delegate.deleteClusterInvocations == 1)
      assert(client.delegate.clusterRunning === false)
    }
  }
  
  test("stop() - error deleting cluster") {
    val e = new Exception("blarg")
    
    withMockClient { delegateClient =>
      
      val client = new LiteralMockDataProcClient(
          delegate = delegateClient, 
          isClusterRunningBody = delegateClient.isClusterRunning, 
          stopClusterBody = throw e)
      
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning === false)
          
      googleRunner.withCluster(clusterConfig0)(42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning === true)
      
      //Make sure we pass through Exceptions thrown when shutting down the cluster, and always
      //shut down the Runner's Executor
      
      val thrown = intercept[Exception] {
        googleRunner.stop()
      }
      
      assert(thrown === e)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
      assert(client.delegate.deleteClusterInvocations == 1)
      assert(client.delegate.clusterRunning === false)
    }
  }
  
  test("stop() - error checking cluster status, error deleting cluster") {
    val checkClusterException = new Exception("nuh")
    val deleteClusterException = new Exception("blarg")
    
    withMockClient { delegateClient =>
      
      val client = new LiteralMockDataProcClient(
          delegate = delegateClient, 
          isClusterRunningBody = throw checkClusterException, 
          stopClusterBody = throw deleteClusterException)
      
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning === false)
          
      googleRunner.withCluster(clusterConfig0)(42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning === true)
      
      //Make sure we still try to shut the cluster down, even if the client throws when checking whether the
      //cluster is running.
      
      val thrown = intercept[Exception] {
        googleRunner.stop()
      }
      
      assert(thrown === deleteClusterException)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
      assert(client.delegate.deleteClusterInvocations == 1)
      assert(client.delegate.clusterRunning === false)
    }
  }
  
  test("run - empty input") {
    withMockRunner { (_, googleRunner, client) =>
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Nil)
      assert(client.deleteClusterInvocations === 0)
      
      val result = {
        googleRunner.run(Set.empty, TestHelpers.DummyJobOracle).toListL.runSyncUnsafe(TestHelpers.defaultWaitTime)
      }
      
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Nil)
      assert(client.deleteClusterInvocations === 0)
      
      assert(result === Nil)
    }
  }
  
  test("run - non-empty input") {
    withMockRunner { (_, googleRunner, client) =>
      val localResources = TestHelpers.localResources
      
      val job1 = mockJob(JobResult.Success)
      val job2 = mockJob(JobResult.Failure)
      val job3 = mockJob(JobResult.CommandResult(0), Some(localResources))
      
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Nil)
      assert(client.deleteClusterInvocations === 0)
      
      val z: Map[LJob, RunData] = Map.empty
      
      val runDataObs = googleRunner.run(Set(job1, job2, job3), TestHelpers.DummyJobOracle)
      
      val jobRuns = waitForT(runDataObs.foldLeft(z)(_ + _).firstL)
      
      assert(client.clusterRunning === true)
      assert(client.startClusterInvocations === Seq(clusterConfigFrom(job1)))
      assert(client.deleteClusterInvocations === 0)
      
      assert(jobRuns(job1) === job1.toReturn)
      assert(jobRuns(job2) === job2.toReturn)

      val job3RunData = jobRuns(job3)
      val job3Result = job3RunData.jobResult.get.asInstanceOf[JobResult.CommandResult]

      assert(job3Result.exitCode === 0)

      val job3Resources = job3RunData.resourcesOpt.get.asInstanceOf[GoogleResources]

      assert(job3Resources.clusterId === clusterId)
      assert(job3Resources.startTime === localResources.startTime)
      assert(job3Resources.endTime === localResources.endTime)
      
      assert(jobRuns.size === 3)
    }
  }
  
  test("run - non-empty input, multiple clusterconfigs") {
    withMockRunner { (mockRunner, googleRunner, client) =>
      val localResources3 = TestHelpers.localResources
      val localResources4 = TestHelpers.localResources
      
      import JobResult._
      
      val settings12 = GoogleSettings(clusterId, clusterConfig0)
      val settings3 = GoogleSettings(clusterId, clusterConfig1)
      val settings4 = GoogleSettings(clusterId, clusterConfig2)
      
      //clusterConfig0
      val job1 = mockJob(result = Success, initialSettings = settings12)
      val job2 = mockJob(result = Failure, initialSettings = settings12)
      //clusterConfig1
      val job3 = mockJob(
          result = CommandResult(0), 
          resources = Some(localResources3), 
          initialSettings = settings3)
      //clusterConfig2
      val job4 = mockJob(
          result = CommandResult(0), 
          resources = Some(localResources4), 
          initialSettings = settings4)
      
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Nil)
      assert(client.deleteClusterInvocations === 0)
      
      val z: Map[LJob, RunData] = Map.empty
      
      val runDataObs = googleRunner.run(Seq(job1, job2, job3, job4), TestHelpers.DummyJobOracle)
      
      val jobRuns = waitForT(runDataObs.foldLeft(z)(_ + _).firstL)
      
      assert(client.clusterRunning === true)
      assert(client.startClusterInvocations.to(Set) === 
        Set(settings12.clusterConfig, settings3.clusterConfig, settings4.clusterConfig))
      assert(client.deleteClusterInvocations === 2)
      
      assert(jobRuns(job1) === job1.toReturn)
      assert(jobRuns(job2) === job2.toReturn)

      def checkCommandResult(job: LJob, expectedLocalResources: LocalResources): Unit = {
        val jobRunData = jobRuns(job)
        val jobResult = jobRunData.jobResult.get.asInstanceOf[JobResult.CommandResult]
  
        assert(jobResult.exitCode === 0)
  
        val jobResources = jobRunData.resourcesOpt.get.asInstanceOf[GoogleResources]
  
        val expectedResources = GoogleResources(
            clusterId = clusterId, 
            startTime = expectedLocalResources.startTime, 
            endTime = expectedLocalResources.endTime)
        
        assert(jobResources === expectedResources)
      }

      checkCommandResult(job3, localResources3)
      checkCommandResult(job4, localResources4)
      
      assert(jobRuns.size === 4)
      
      import Maps.Implicits.MapOps
      
      assert(jobRuns.strictMapValues(_.settings) === Map(
          job1 -> settings12, job2 -> settings12, job3 -> settings3, job4 -> settings4))
    }
  }
  
  test("run - non-empty input, error starting cluster") {
    withMockClient { delegateClient =>
      val e = new Exception("blarg")
      
      val client = new LiteralMockDataProcClient(
          delegate = delegateClient, 
          isClusterRunningBody = delegateClient.isClusterRunning, 
          startClusterBody = throw e)
      
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      val localResources = TestHelpers.localResources
      
      val job1 = mockJob(JobResult.Success)
      val job2 = mockJob(JobResult.Failure)
      val job3 = mockJob(JobResult.CommandResult(0), Some(localResources))
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Nil)
      assert(client.delegate.deleteClusterInvocations === 0)
      
      val thrown = intercept[Exception] {
        waitForT(googleRunner.run(Set(job1, job2, job3), TestHelpers.DummyJobOracle).firstL)
      }
      
      assert(thrown === e)
      
      assert(client.delegate.clusterRunning === true)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfigFrom(job1)))
      assert(client.delegate.deleteClusterInvocations === 0)
    }
  }
  
  test("deleteClusterIfNecessary - no errors") {
    withMockRunner { (_, googleRunner, client) =>
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Nil)
      assert(client.deleteClusterInvocations === 0)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Nil)
      assert(client.deleteClusterInvocations === 0)
      assert(client.isClusterRunningInvocations === 1)
      
      val job1 = mockJob(JobResult.Success)
      
      waitForT(googleRunner.run(Set(job1), TestHelpers.DummyJobOracle).firstL)
      
      assert(client.isClusterRunningInvocations === 2)
      
      assert(client.clusterRunning === true)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Seq(clusterConfigFrom(job1)))
      assert(client.deleteClusterInvocations === 1)
      assert(client.isClusterRunningInvocations === 3)
    }
  }
  
  test("deleteClusterIfNecessary - error checking cluster status") {
    withMockClient { delegateClient =>
      val client = new LiteralMockDataProcClient(delegateClient, ???)
      
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Nil)
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 0)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Nil)
      assert(client.delegate.deleteClusterInvocations === 1)
      assert(client.delegate.isClusterRunningInvocations === 1)
      
      val job1 = mockJob(JobResult.Success)
      
      waitForT(googleRunner.run(Set(job1), TestHelpers.DummyJobOracle).firstL)
      
      assert(client.delegate.isClusterRunningInvocations === 2)
      
      assert(client.delegate.clusterRunning === true)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfigFrom(job1)))
      assert(client.delegate.deleteClusterInvocations === 2)
      assert(client.delegate.isClusterRunningInvocations === 3)
    }
  }
  
  test("deleteClusterIfNecessary - error deleting cluster") {
    import GoogleCloudChunkRunnerTest.LiteralMockDataProcClient
    
    withMockClient { delegateClient =>
      val client = new LiteralMockDataProcClient(
          delegateClient, 
          delegateClient.clusterRunning,
          (),
          throw new Exception)
      
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Nil)
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 0)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Nil)
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 1)
      
      val job1 = mockJob(JobResult.Success)
      
      waitForT(googleRunner.run(Set(job1), TestHelpers.DummyJobOracle).firstL)
      
      assert(client.delegate.isClusterRunningInvocations === 2)
      
      assert(client.delegate.clusterRunning === true)
      
      intercept[Exception] {
        googleRunner.deleteClusterIfNecessary()
      }
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfigFrom(job1)))
      assert(client.delegate.deleteClusterInvocations === 1)
      assert(client.delegate.isClusterRunningInvocations === 3)
    }
  }
  
  test("startClusterIfNecessary - no errors") {
    withMockRunner { (_, googleRunner, client) =>
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Nil)
      assert(client.deleteClusterInvocations === 0)
      
      googleRunner.startClusterIfNecessary(clusterConfig0)
      
      assert(client.clusterRunning === true)
      assert(client.startClusterInvocations === Seq(clusterConfig0))
      assert(client.deleteClusterInvocations === 0)
      assert(client.isClusterRunningInvocations === 1)
      
      val job1 = mockJob(JobResult.Success)
      
      waitForT(googleRunner.run(Set(job1), TestHelpers.DummyJobOracle).firstL)
      
      assert(client.clusterRunning === true)
      
      googleRunner.startClusterIfNecessary(clusterConfig1)
      
      assert(client.clusterRunning === true)
      assert(client.startClusterInvocations === Seq(clusterConfigFrom(job1)))
      assert(client.deleteClusterInvocations === 0)
      assert(client.isClusterRunningInvocations === 3)
      
      client.stopCluster()
      
      assert(client.clusterRunning === false)
      assert(client.startClusterInvocations === Seq(clusterConfigFrom(job1)))
      assert(client.deleteClusterInvocations === 1)
      assert(client.isClusterRunningInvocations === 3)
      
      googleRunner.startClusterIfNecessary(clusterConfig1)
      
      assert(client.clusterRunning === true)
      assert(client.startClusterInvocations === Seq(clusterConfig0, clusterConfig1))
      assert(client.deleteClusterInvocations === 1)
      assert(client.isClusterRunningInvocations === 4)
    }
  }
  
  test("startClusterIfNecessary - error checking cluster status") {
    withMockClient { delegateClient =>
      val client = new LiteralMockDataProcClient(delegateClient, ???)
      
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Nil)
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 0)
      
      googleRunner.startClusterIfNecessary(clusterConfig0)
      
      assert(client.delegate.clusterRunning === true)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfig0))
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 1)
      
      val job1 = mockJob(JobResult.Success)
      
      waitForT(googleRunner.run(Set(job1), TestHelpers.DummyJobOracle).firstL)
      
      assert(client.delegate.clusterRunning === true)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfig0, clusterConfig0))
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 2)
      
      googleRunner.startClusterIfNecessary(clusterConfig0)
      
      assert(client.delegate.clusterRunning === true)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfig0, clusterConfig0, clusterConfig0))
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 3)
      
      client.stopCluster()
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.deleteClusterInvocations === 1)
      
      googleRunner.startClusterIfNecessary(clusterConfig1)
      
      assert(client.delegate.clusterRunning === true)
      assert(client.delegate.startClusterInvocations.size === 4)
      assert(client.delegate.startClusterInvocations === 
                Seq(clusterConfig0, clusterConfig0, clusterConfig0, clusterConfig1))
      assert(client.delegate.deleteClusterInvocations === 1)
      assert(client.delegate.isClusterRunningInvocations === 4)
    }
  }
  
  test("startClusterIfNecessary - error starting cluster") {
    import GoogleCloudChunkRunnerTest.LiteralMockDataProcClient
    
    withMockClient { delegateClient =>
      val client = new LiteralMockDataProcClient(
          delegate = delegateClient, 
          isClusterRunningBody = delegateClient.clusterRunning,
          startClusterBody = throw new Exception,
          stopClusterBody = ())
      
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(client.delegate.clusterRunning === false)
      assert(client.delegate.startClusterInvocations === Nil)
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 0)
      
      intercept[Exception] {
        googleRunner.startClusterIfNecessary(clusterConfig1)
      }
      
      assert(client.delegate.clusterRunning === true)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfig1))
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 1)
      
      val job1 = mockJob(JobResult.Success)
      
      waitForT(googleRunner.run(Set(job1), TestHelpers.DummyJobOracle).firstL)
      
      assert(client.delegate.clusterRunning === true)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfig1))
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 2)
      
      googleRunner.startClusterIfNecessary(clusterConfig0)
      
      assert(client.delegate.clusterRunning === true)
      assert(client.delegate.startClusterInvocations === Seq(clusterConfig1))
      assert(client.delegate.deleteClusterInvocations === 0)
      assert(client.delegate.isClusterRunningInvocations === 3)
    }
  }
  
  private def withMockClient[A](f: MockDataProcClient => A): A = f(new MockDataProcClient)
  
  private def withMockRunner[A](f: (MockChunkRunner, GoogleCloudChunkRunner, MockDataProcClient) => A): A = {
    withMockClient { client => 
      val localRunner = AsyncLocalChunkRunner(ExecutionConfig.default, 1)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      val mockRunner = MockChunkRunner(googleRunner)
      
      try {
        f(mockRunner, googleRunner, client)
      } finally {
        googleRunner.stop()
      }
    }
  }
}

object GoogleCloudChunkRunnerTest {

  final class LiteralMockDataProcClient(
      val delegate: MockDataProcClient,
      isClusterRunningBody: => Boolean,
      startClusterBody: => Any = (),
      stopClusterBody: => Any = ()) extends DataProcClient {
    
    override def isClusterRunning: Boolean = {
      delegate.isClusterRunning
      
      isClusterRunningBody
    }
    
    override def startCluster(clusterConfig: ClusterConfig): Unit = {
      delegate.startCluster(clusterConfig)
      
      startClusterBody
    }
    
    override def stopCluster(): Unit = {
      delegate.stopCluster()
      
      stopClusterBody
    }
  }
  
  final class MockDataProcClient extends DataProcClient {
    private[this] var _startClusterInvocations: Seq[ClusterConfig] = Vector.empty
    private[this] var _deleteClusterInvocations: Int = 0
    private[this] var _isClusterRunningInvocations: Int = 0
    private[this] var _clusterRunning: Boolean = false
    
    def startClusterInvocations: Seq[ClusterConfig] = lock.synchronized(_startClusterInvocations)
    def deleteClusterInvocations: Int = lock.synchronized(_deleteClusterInvocations)
    def isClusterRunningInvocations: Int = lock.synchronized(_isClusterRunningInvocations)
    def clusterRunning: Boolean = lock.synchronized(_clusterRunning)
    
    private[this] val lock = new AnyRef 
    
    def reset(): Unit = lock.synchronized {
      _startClusterInvocations = Vector.empty
      _deleteClusterInvocations = 0
      _isClusterRunningInvocations = 0
      _clusterRunning = false
    }
    
    override def stopCluster(): Unit = lock.synchronized {
      _deleteClusterInvocations += 1
      
      _clusterRunning = false
    }
  
    override def isClusterRunning: Boolean = lock.synchronized {
      _isClusterRunningInvocations += 1
      
      _clusterRunning
    }
    
    override def startCluster(clusterConfig: ClusterConfig): Unit = lock.synchronized {
      _startClusterInvocations = _startClusterInvocations :+ clusterConfig
      
      _clusterRunning = true
    }
  }
}
