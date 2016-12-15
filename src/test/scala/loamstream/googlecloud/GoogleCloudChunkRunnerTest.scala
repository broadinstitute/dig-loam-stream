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

/**
 * @author clint
 * Dec 15, 2016
 */
final class GoogleCloudChunkRunnerTest extends FunSuite {
  //scalastyle:off magic.number
  
  import GoogleCloudChunkRunnerTest.MockDataProcClient
  import Futures.waitFor
  import ObservableEnrichments._

  test("runSingle") {
    import GoogleCloudChunkRunner.runSingle
    
    val jobResult =  JobState.CommandResult(42)
    
    val job = MockJob(jobResult)
    
    val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
    
    val mockRunner = MockChunkRunner(localRunner)
    
    assert(mockRunner.chunks() === Nil)
    
    val result = runSingle(mockRunner)(job)
    
    assert(mockRunner.chunks() === Seq(Set(job)))
    
    assert(result === Map(job -> jobResult))
  }
  
  test("withCluster") {
    import GoogleCloudChunkRunner.withCluster
    
    withMockClient { client =>
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      
      val result = withCluster(client) {
        assert(client.clusterRunning() === true)
        assert(client.startClusterInvocations() === 1)
        assert(client.deleteClusterInvocations() === 0)
        
        42
      }
    
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 1)
      assert(client.deleteClusterInvocations() === 1)
      
      assert(result === 42)
    }
  }
  
  test("runJobsSequentially") {
    val job1 = MockJob(JobState.Succeeded)
    val job2 = MockJob(JobState.Failed)
    val job3 = MockJob(JobState.CommandResult(0))
    
    val expected = Map(job1 -> job1.toReturn, job2 -> job2.toReturn, job3 -> job3.toReturn)
    
    withMockRunner { (mockRunner, actualGoogleRunner, client) =>
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      
      val resultObs = actualGoogleRunner.runJobsSequentially(Set(job1, job2, job3))
      
      val result = waitFor(resultObs.lastAsFuture)
      
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 1)
      assert(client.deleteClusterInvocations() === 1)
      
      assert(result === expected)
    }
  }
  
  test("stop()") {
    withMockRunner { (_, googleRunner, _) =>
      assert(googleRunner.singleThreadedExecutor.isShutdown === false)
      
      googleRunner.stop()
      
      assert(googleRunner.singleThreadedExecutor.isShutdown === true)
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
      val job1 = MockJob(JobState.Succeeded)
      val job2 = MockJob(JobState.Failed)
      val job3 = MockJob(JobState.CommandResult(0))
      
      val expected = Map(job1 -> job1.toReturn, job2 -> job2.toReturn, job3 -> job3.toReturn)
      
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 0)
      assert(client.deleteClusterInvocations() === 0)
      
      val result = waitFor(googleRunner.run(Set(job1, job2, job3)).lastAsFuture)
      
      assert(client.clusterRunning() === false)
      assert(client.startClusterInvocations() === 1)
      assert(client.deleteClusterInvocations() === 1)
      
      assert(result === expected)
    }
  }
  
  private def withMockClient[A](f: MockDataProcClient => A): A = f(new MockDataProcClient)
  
  private def withMockRunner[A](f: (MockChunkRunner, GoogleCloudChunkRunner, MockDataProcClient) => A): A = {
    withMockClient { client => 
      val localRunner = AsyncLocalChunkRunner(1)(ExecutionContext.global)
      
      val googleRunner = GoogleCloudChunkRunner(client, localRunner)
      
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