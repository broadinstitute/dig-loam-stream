package loamstream.googlecloud

import loamstream.util.ValueBox
import org.scalatest.FunSuite
import loamstream.model.execute.MockChunkRunner
import loamstream.model.execute.AsyncLocalChunkRunner
import scala.concurrent.ExecutionContext
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobState
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import loamstream.model.execute.Resources.LocalResources
import loamstream.TestHelpers
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.ProvidesEnvAndResources
import loamstream.model.jobs.JobState.CommandResult
import loamstream.model.jobs.LJob


/**
 * @author clint
 * Dec 15, 2016
 */
final class GoogleCloudChunkRunnerTest extends FunSuite {
  //scalastyle:off magic.number
  
  import GoogleCloudChunkRunnerTest.MockDataProcClient
  import GoogleCloudChunkRunnerTest.LiteralMockDataProcClient
  import Futures.waitFor
  import ObservableEnrichments._

  private val clusterId = "some-cluster-id"
  
  private val googleConfig = {
    import TestHelpers.path
    
    GoogleCloudConfig(path("gcloud"), "some-project-id", clusterId, path("creds-file"))
  }
  
  test("addCluster") {
    import GoogleCloudChunkRunner.addCluster
    
    val clusterId = "foo"
    
    val localResources = TestHelpers.localResources
    
    val ugerResources = ProvidesEnvAndResources.mockUgerResources
    val googleResources = ProvidesEnvAndResources.mockGoogleResources
    
    val job1 = MockJob(CommandResult(0, Some(localResources)))
    val job2 = MockJob(CommandResult(1, Some(ugerResources)))
    val job3 = MockJob(CommandResult(2, Some(googleResources)))
    
    val input: Map[LJob, JobState] = Map(job1 -> job1.toReturn, job2 -> job2.toReturn, job3 -> job3.toReturn)
    
    val result = addCluster(clusterId)(input)
    
    val job1Result = result(job1).asInstanceOf[CommandResult]
    
    assert(job1Result.exitStatus === 0)
    assert(job1Result.resources.get === GoogleResources(clusterId, localResources.startTime, localResources.endTime))
    
    assert(result(job2) === input(job2))
    assert(result(job3) === input(job3))
  }
  
  test("withCluster") {
    withMockRunner { (_, googleRunner, mockClient) =>
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(mockClient.clusterRunning() === false)
      assert(mockClient.startClusterInvocations() === 0)
      assert(mockClient.deleteClusterInvocations() === 0)

      googleRunner.withCluster(mockClient) {
        assert(mockClient.clusterRunning() === true)
        assert(mockClient.startClusterInvocations() === 1)
        assert(mockClient.deleteClusterInvocations() === 0)
      }
      
      googleRunner.withCluster(mockClient) {
        assert(mockClient.clusterRunning() === true)
        assert(mockClient.startClusterInvocations() === 1)
        assert(mockClient.deleteClusterInvocations() === 0)
      }
      
      val result = googleRunner.withCluster(mockClient) {
        assert(mockClient.clusterRunning() === true)
        assert(mockClient.startClusterInvocations() === 1)
        assert(mockClient.deleteClusterInvocations() === 0)
        
        42
      }
      
      assert(result === 42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(mockClient.clusterRunning() === true)
      assert(mockClient.startClusterInvocations() === 1)
      assert(mockClient.deleteClusterInvocations() === 0)
    } 
  }
  
  test("runJobsSequentially") {
    val localResources = TestHelpers.localResources
    
    val job1 = MockJob(JobState.Succeeded)
    val job2 = MockJob(JobState.Failed())
    val job3 = MockJob(JobState.CommandResult(0, Some(localResources)))
    
    val expected = Map(job1 -> job1.toReturn, job2 -> job2.toReturn, job3 -> job3.toReturn)
    
    withMockRunner { (mockRunner, actualGoogleRunner, client) =>
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      
      val resultObs = actualGoogleRunner.runJobsSequentially(Set(job1, job2, job3))
      
      val result = waitFor(resultObs.lastAsFuture)
      
      assert(result(job1) === job1.toReturn)
      assert(result(job2) === job2.toReturn)
      
      val job3Result = result(job3).asInstanceOf[JobState.CommandResult]
      
      assert(job3Result.exitStatus === 0)

      val job3Resources = job3Result.resources.get.asInstanceOf[GoogleResources]
      
      assert(job3Resources.cluster === clusterId)
      assert(job3Resources.startTime === localResources.startTime)
      assert(job3Resources.endTime === localResources.endTime)
      
      assert(result.size === 3)
    }
  }
  
  test("stop()") {
    withMockRunner { (_, googleRunner, mockClient) =>
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(mockClient.clusterRunning() === false)
          
      googleRunner.withCluster(mockClient)(42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(mockClient.clusterRunning() === true)
      
      googleRunner.stop()
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
      assert(mockClient.clusterRunning() === false)
    }
  }
  
  test("stop() - error checking cluster status") {
    withMockClient { delegateClient =>
      
      val client = new LiteralMockDataProcClient(delegateClient, ???)
      
      val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning() === false)
          
      googleRunner.withCluster(client)(42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning() === true)
      
      //Make sure the cluster is still shut down, even if the client throws when checking whether the
      //cluster is running.
      
      googleRunner.stop()
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
      assert(client.delegate.deleteClusterInvocations() == 1)
      assert(client.delegate.clusterRunning() === false)
    }
  }
  
  test("stop() - error deleting cluster") {
    val e = new Exception("blarg")
    
    withMockClient { delegateClient =>
      
      val client = new LiteralMockDataProcClient(
          delegate = delegateClient, 
          isClusterRunningBody = delegateClient.isClusterRunning, 
          deleteClusterBody = throw e)
      
      val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning() === false)
          
      googleRunner.withCluster(client)(42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning() === true)
      
      //Make sure we pass through Exceptions thrown when shutting down the cluster, and always
      //shut down the Runner's Executor
      
      val thrown = intercept[Exception] {
        googleRunner.stop()
      }
      
      assert(thrown === e)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
      assert(client.delegate.deleteClusterInvocations() == 1)
      assert(client.delegate.clusterRunning() === false)
    }
  }
  
  test("stop() - error checking cluster status, error deleting cluster") {
    val checkClusterException = new Exception("nuh")
    val deleteClusterException = new Exception("blarg")
    
    withMockClient { delegateClient =>
      
      val client = new LiteralMockDataProcClient(
          delegate = delegateClient, 
          isClusterRunningBody = throw checkClusterException, 
          deleteClusterBody = throw deleteClusterException)
      
      val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning() === false)
          
      googleRunner.withCluster(client)(42)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      assert(client.delegate.clusterRunning() === true)
      
      //Make sure we still try to shut the cluster down, even if the client throws when checking whether the
      //cluster is running.
      
      val thrown = intercept[Exception] {
        googleRunner.stop()
      }
      
      assert(thrown === deleteClusterException)
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
      assert(client.delegate.deleteClusterInvocations() == 1)
      assert(client.delegate.clusterRunning() === false)
    }
  }
  
  test("run - empty input") {
    withMockRunner { (_, googleRunner, client) =>
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      
      val result = waitFor(googleRunner.run(Set.empty).lastAsFuture)
      
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      
      assert(result === Map.empty)
    }
  }
  
  test("run - non-empty input") {
    withMockRunner { (_, googleRunner, client) =>
      val localResources = TestHelpers.localResources
      
      val job1 = MockJob(JobState.Succeeded)
      val job2 = MockJob(JobState.Failed())
      val job3 = MockJob(JobState.CommandResult(0, Some(localResources)))
      
      val expected = Map(job1 -> job1.toReturn, job2 -> job2.toReturn, job3 -> job3.toReturn)
      
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      
      val result = waitFor(googleRunner.run(Set(job1, job2, job3)).lastAsFuture)
      
      assert(client.clusterRunning() === true)
      assert(client.startClusterInvocations() === 1)
      assert(client.deleteClusterInvocations() === 0)
      
      assert(result(job1) === job1.toReturn)
      assert(result(job2) === job2.toReturn)
      
      val job3Result = result(job3).asInstanceOf[JobState.CommandResult]
      
      assert(job3Result.exitStatus === 0)

      val job3Resources = job3Result.resources.get.asInstanceOf[GoogleResources]

      assert(job3Resources.cluster === clusterId)
      assert(job3Resources.startTime === localResources.startTime)
      assert(job3Resources.endTime === localResources.endTime)
      
      assert(result.size === 3)
    }
  }
  
  test("run - non-empty input, error starting cluster") {
    withMockClient { delegateClient =>
      val e = new Exception("blarg")
      
      val client = new LiteralMockDataProcClient(
          delegate = delegateClient, 
          isClusterRunningBody = delegateClient.isClusterRunning, 
          startClusterBody = throw e)
      
      val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      val localResources = TestHelpers.localResources
      
      val job1 = MockJob(JobState.Succeeded)
      val job2 = MockJob(JobState.Failed())
      val job3 = MockJob(JobState.CommandResult(0, Some(localResources)))
      
      assert(client.delegate.clusterRunning() === false)
      assert(client.delegate.startClusterInvocations() === 0)
      assert(client.delegate.deleteClusterInvocations() === 0)
      
      val thrown = intercept[Exception] {
        waitFor(googleRunner.run(Set(job1, job2, job3)).lastAsFuture)
      }
      
      assert(thrown === e)
      
      assert(client.delegate.clusterRunning() === true)
      assert(client.delegate.startClusterInvocations() === 1)
      assert(client.delegate.deleteClusterInvocations() === 0)
    }
  }
  
  test("deleteClusterIfNecessary - no errors") {
    withMockRunner { (_, googleRunner, client) =>
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      assert(client.isClusterRunningInvocations() === 1)
      
      val job1 = MockJob(JobState.Succeeded)
      
      waitFor(googleRunner.run(Set(job1)).lastAsFuture)
      
      assert(client.clusterRunning() === true)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 1)
      assert(client.deleteClusterInvocations() === 1)
      assert(client.isClusterRunningInvocations() === 2)
    }
  }
  
  test("deleteClusterIfNecessary - error checking cluster status") {
    withMockClient { delegateClient =>
      val client = new LiteralMockDataProcClient(delegateClient, ???)
      
      val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(client.delegate.clusterRunning() === false)
      assert(client.delegate.startClusterInvocations() === 0)
      assert(client.delegate.deleteClusterInvocations() === 0)
      assert(client.delegate.isClusterRunningInvocations() === 0)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.delegate.clusterRunning() === false)
      assert(client.delegate.startClusterInvocations() === 0)
      assert(client.delegate.deleteClusterInvocations() === 1)
      assert(client.delegate.isClusterRunningInvocations() === 1)
      
      val job1 = MockJob(JobState.Succeeded)
      
      waitFor(googleRunner.run(Set(job1)).lastAsFuture)
      
      assert(client.delegate.clusterRunning() === true)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.delegate.clusterRunning() === false)
      assert(client.delegate.startClusterInvocations() === 1)
      assert(client.delegate.deleteClusterInvocations() === 2)
      assert(client.delegate.isClusterRunningInvocations() === 2)
    }
  }
  
  test("deleteClusterIfNecessary - error deleting cluster") {
    import GoogleCloudChunkRunnerTest.LiteralMockDataProcClient
    
    withMockClient { delegateClient =>
      val client = new LiteralMockDataProcClient(
          delegateClient, 
          delegateClient.clusterRunning(),
          (),
          throw new Exception)
      
      val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      assert(client.delegate.clusterRunning() === false)
      assert(client.delegate.startClusterInvocations() === 0)
      assert(client.delegate.deleteClusterInvocations() === 0)
      assert(client.delegate.isClusterRunningInvocations() === 0)
      
      googleRunner.deleteClusterIfNecessary()
      
      assert(client.delegate.clusterRunning() === false)
      assert(client.delegate.startClusterInvocations() === 0)
      assert(client.delegate.deleteClusterInvocations() === 0)
      assert(client.delegate.isClusterRunningInvocations() === 1)
      
      val job1 = MockJob(JobState.Succeeded)
      
      waitFor(googleRunner.run(Set(job1)).lastAsFuture)
      
      assert(client.delegate.clusterRunning() === true)
      
      intercept[Exception] {
        googleRunner.deleteClusterIfNecessary()
      }
      
      assert(client.delegate.clusterRunning() === false)
      assert(client.delegate.startClusterInvocations() === 1)
      assert(client.delegate.deleteClusterInvocations() === 1)
      assert(client.delegate.isClusterRunningInvocations() === 2)
    }
  }
  
  private def withMockClient[A](f: MockDataProcClient => A): A = f(new MockDataProcClient)
  
  private def withMockRunner[A](f: (MockChunkRunner, GoogleCloudChunkRunner, MockDataProcClient) => A): A = {
    withMockClient { client => 
      val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
      
      val googleRunner = GoogleCloudChunkRunner(client, googleConfig, localRunner)
      
      val mockRunner = MockChunkRunner(googleRunner)
      
      try {
        f(mockRunner, googleRunner, client)
      } finally {
        googleRunner.stop()
      }
    }
  }

  //scalastyle:on magic.number
}

object GoogleCloudChunkRunnerTest {
  //scalastyle:off magic.number
  
  final class LiteralMockDataProcClient(
      val delegate: MockDataProcClient,
      isClusterRunningBody: => Boolean,
      startClusterBody: => Any = (),
      deleteClusterBody: => Any = ()) extends DataProcClient {
    
    override def isClusterRunning: Boolean = {
      delegate.isClusterRunning
      
      isClusterRunningBody
    }
    
    override def startCluster(): Unit = {
      delegate.startCluster()
      
      startClusterBody
    }
    
    override def deleteCluster(): Unit = {
      delegate.deleteCluster()
      
      deleteClusterBody
    }
  }
  
  final class MockDataProcClient extends DataProcClient {
    val startClusterInvocations: ValueBox[Int] = ValueBox(0)
    val deleteClusterInvocations: ValueBox[Int] = ValueBox(0)
    val isClusterRunningInvocations: ValueBox[Int] = ValueBox(0)
    
    val clusterRunning: ValueBox[Boolean] = ValueBox(false)
    
    def reset(): Unit = {
      startClusterInvocations.update(0)
      deleteClusterInvocations.update(0)
      isClusterRunningInvocations.update(0)
      clusterRunning := false
    }
    
    override def deleteCluster(): Unit = {
      deleteClusterInvocations.mutate(_ + 1)
      
      clusterRunning := false
    }
  
    override def isClusterRunning: Boolean = {
      isClusterRunningInvocations.mutate(_ + 1)
      
      clusterRunning()
    }
    
    override def startCluster(): Unit = {
      startClusterInvocations.mutate(_ + 1)
      
      clusterRunning := true
    }
  }
  
  //scalastyle:on magic.number
}
