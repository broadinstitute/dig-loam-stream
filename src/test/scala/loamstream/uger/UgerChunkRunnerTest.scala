package loamstream.uger

import com.typesafe.config.ConfigFactory
import loamstream.conf.UgerConfig
import loamstream.model.jobs.NoOpJob
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext
import scala.util.Success
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobState
import LJob.SimpleFailure
import LJob.SimpleSuccess
import loamstream.model.jobs.commandline.CommandLineStringJob
import java.nio.file.Paths
import loamstream.util.Observables
import java.nio.file.Path
import scala.util.Try
import scala.concurrent.duration.Duration
import loamstream.model.jobs.JobState.NotStarted
import rx.lang.scala.Observable
import loamstream.util.ValueBox
import loamstream.util.Sequence
import loamstream.util.Loggable

/**
 * Created by kyuksel on 7/25/16.
 */
final class UgerChunkRunnerTest extends FunSuite {
  private val config = UgerConfig.fromConfig(ConfigFactory.load("loamstream-test.conf")).get
  private val client = {
    import loamstream.uger.JobStatus._

    MockDrmaaClient(Success(Queued), Success(Running), Success(Done))
  }
  private val runner = UgerChunkRunner(config, client)

  import Futures.waitFor
  import ObservableEnrichments._

  test("NoOpJob is not attempted to be executed") {
    val noOpJob = NoOpJob(Set.empty)
    val result = runner.run(Set(noOpJob))(ExecutionContext.global)

    assert(waitFor(result.lastAsFuture) === Map())
  }

  test("No failures when empty set of jobs is presented") {
    val result = runner.run(Set.empty)(ExecutionContext.global)

    assert(waitFor(result.lastAsFuture) === Map())
  }

  test("makeAllFailureMap") {

    val e = new Exception("foo")

    import UgerChunkRunner.makeAllFailureMap

    assert(waitFor(makeAllFailureMap(Nil, None).firstAsFuture) == Map.empty)
    assert(waitFor(makeAllFailureMap(Nil, Some(e)).firstAsFuture) == Map.empty)

    val j1 = MockJob(SimpleSuccess("asdf"))
    val j2 = MockJob(SimpleSuccess("nuh"))

    {
      val failure = SimpleFailure("Couldn't submit jobs to UGER")

      val expected = Map(j1 -> failure, j2 -> failure)

      assert(waitFor(makeAllFailureMap(Seq(j1, j2), None).firstAsFuture) == expected)
    }

    {
      val failure = SimpleFailure("Couldn't submit jobs to UGER: foo")

      val expected = Map(j1 -> failure, j2 -> failure)

      assert(waitFor(makeAllFailureMap(Seq(j1, j2), Some(e)).firstAsFuture) == expected)
    }
  }

  test("handleFailedSubmission") {
    val j1 = MockJob(SimpleSuccess("asdf"))
    val j2 = MockJob(SimpleSuccess("nuh"))

    val e = new Exception("foo")

    import UgerChunkRunner.handleFailedSubmission

    val jobs = Seq(j1, j2)

    import JobState._

    jobs.foreach(_.updateAndEmitJobState(NotStarted))

    assert(j1.state == NotStarted)
    assert(j2.state == NotStarted)

    val states = Observables.sequence(jobs.map(_.states))

    val result = handleFailedSubmission(jobs, e)

    assert(j1.state == Failed)
    assert(j2.state == Failed)

    //NB: Use drop(1) to skip the NotStarted states
    assert(waitFor(states.drop(1).firstAsFuture) == Seq(Failed, Failed))

    val failure = SimpleFailure("Couldn't submit jobs to UGER: foo")

    val expected = Map(j1 -> failure, j2 -> failure)

    assert(waitFor(result.firstAsFuture) == expected)
  }

  test("toResultMap - jobs finish successully") {

    val j1 = MockJob(SimpleSuccess("1"), name = "Job1")
    val j2 = MockJob(SimpleSuccess("2"), name = "Job2")
    
    import JobStatus._
    
    val j1Statuses = Seq(Queued, Queued, Queued, Queued, Running, Done)
    val j2Statuses = Seq(Queued, Queued, Queued, Running, Running, Running, Running, Done)
    
    /*
     * Job2:
				 Invocation(Job2,ForkJoinPool-1-worker-11,0,Queued)
				 Invocation(Job2,ForkJoinPool-1-worker-7,1,Queued)
				 Invocation(Job2,ForkJoinPool-1-worker-9,2,Queued)
				 Invocation(Job2,ForkJoinPool-1-worker-5,3,Running)
				 Invocation(Job2,ForkJoinPool-1-worker-9,4,Running)
				 Invocation(Job2,ForkJoinPool-1-worker-5,5,Running)
				 Invocation(Job2,ForkJoinPool-1-worker-9,6,Running)
			 Job1:
			 	 Invocation(Job1,ForkJoinPool-1-worker-9,0,Queued)
			 	 Invocation(Job1,ForkJoinPool-1-worker-5,1,Queued)
			 	 Invocation(Job1,ForkJoinPool-1-worker-7,2,Queued)
			 	 Invocation(Job1,ForkJoinPool-1-worker-9,3,Queued)
			 	 Invocation(Job1,ForkJoinPool-1-worker-5,4,Running)
			 	 Invocation(Job1,ForkJoinPool-1-worker-9,5,Done)
     */
    
    val j1States = j1Statuses.map(JobStatus.toJobState)
    val j2States = j2Statuses.map(JobStatus.toJobState)
    
    val drmaaClient = UgerChunkRunnerTest.MockDrmaaClient(
        Right(Map(j1 -> j1.name, j2 -> j2.name)),
        Map(j1.name -> j1Statuses, j2.name -> j2Statuses))
    
    import scala.concurrent.ExecutionContext.Implicits.global
            
    def statesFrom(j: LJob): Observable[Seq[JobState]] = j.states.until(_.isFinished).to[Seq]
    
    val observedJ1States = statesFrom(j1)
    val observedJ2States = statesFrom(j2)
    
    val result = UgerChunkRunner.toResultMap(drmaaClient, 9.99, Map(j1.name -> j1, j2.name -> j2))

    val expectedJobStates = Seq(JobState.Running, JobState.Succeeded)
    
    assert(waitFor(observedJ1States.firstAsFuture) == expectedJobStates)
    assert(waitFor(observedJ2States.firstAsFuture) == expectedJobStates)
    
    val actualResults = waitFor(result.firstAsFuture)
    
    val expected = Map(j1 -> SimpleSuccess(j1.toString), j2 -> SimpleSuccess(j2.toString))
    
    assert(actualResults == expected)
    
    /*
    private[uger] def toResultMap(
      drmaaClient: DrmaaClient,
      pollingFrequencyInHz: Double,
      jobsById: Map[String, LJob])(implicit context: ExecutionContext): Observable[Map[LJob, Result]] = {
    
    val poller = Poller.drmaa(drmaaClient)

    def statuses(jobId: String) = Jobs.monitor(poller, pollingFrequencyInHz)(jobId)

    val jobsToStates: Iterable[(LJob, Observable[JobState])] = for {
      (jobId, job) <- jobsById
    } yield {
      val states = statuses(jobId).map(toJobState)
      
      job -> states
    }

    for {
      (job, states) <- jobsToStates
    } {
      states.foreach(job.updateAndEmitJobState)
    }
    
    val jobsToFutureResults: Iterable[(LJob, Observable[Result])] = for {
      (job, jobStates) <- jobsToStates
    } yield {
      val resultObservable = jobStates.last.map(resultFrom(job))

      job -> resultObservable
    }

    Observables.toMap(jobsToFutureResults)
  }
     */
  }

  test("resultFrom") {
    import JobState._
    import UgerChunkRunner.resultFrom

    val job = MockJob(SimpleSuccess("asdf"))

    assert(resultFrom(job)(Succeeded) == SimpleSuccess(job.toString))

    val failure = SimpleFailure(job.toString)

    assert(resultFrom(job)(NotStarted) == failure)
    assert(resultFrom(job)(Running) == failure)
    assert(resultFrom(job)(Failed) == failure)
    assert(resultFrom(job)(Unknown) == failure)
  }

  test("isAcceptableJob") {
    import UgerChunkRunner.isAcceptableJob

    assert(!isAcceptableJob(MockJob(SimpleSuccess(""))))

    assert(isAcceptableJob(NoOpJob(Set.empty)))

    assert(isAcceptableJob(CommandLineStringJob("foo", Paths.get("."))))
  }

  test("isNoOpJob") {
    import UgerChunkRunner.isNoOpJob

    assert(!isNoOpJob(MockJob(SimpleSuccess(""))))

    assert(isNoOpJob(NoOpJob(Set.empty)))

    assert(!isNoOpJob(CommandLineStringJob("foo", Paths.get("."))))
  }

  test("isCommandLineJob") {
    import UgerChunkRunner.isCommandLineJob

    assert(!isCommandLineJob(MockJob(SimpleSuccess(""))))

    assert(!isCommandLineJob(NoOpJob(Set.empty)))

    assert(isCommandLineJob(CommandLineStringJob("foo", Paths.get("."))))
  }
}

object UgerChunkRunnerTest extends Loggable {
  final case class MockDrmaaClient(
      idsForJobs: Either[Exception, Map[LJob, String]],
      val _statusesByJobId: Map[String, Seq[JobStatus]]) extends DrmaaClient {

    private[this] val isShutdown: ValueBox[Boolean] = ValueBox(false)

    private[this] val statusesByJobId: ValueBox[Map[String, Seq[JobStatus]]] = ValueBox(_statusesByJobId)
    
    override def submitJob(
      pathToScript: Path,
      pathToUgerOutput: Path,
      jobName: String,
      numTasks: Int = 1): DrmaaClient.SubmissionResult = {

      idsForJobs match {
        case Left(e)          => DrmaaClient.SubmissionFailure(idsForJobs.left.get)
        case Right(jobsToIds) => DrmaaClient.SubmissionSuccess(jobsToIds.values.toSeq)
      }
    }

    override def statusOf(jobId: String): Try[JobStatus] = {
      Try(statusesByJobId.value.apply(jobId).head)
    }
    
    val is: Map[String, Sequence[Int]] = _statusesByJobId.map { case (jobId, _) => jobId -> Sequence() }
    
    final case class Invocation(jobId: String, threadId: String, num: Int, result: JobStatus)
    
    val invocations: ValueBox[Seq[Invocation]] = ValueBox(Vector.empty)

    override def waitFor(jobId: String, timeout: Duration): Try[JobStatus] = {
      Try {
        try { 
        val result = statusOf(jobId)
        
        invocations.mutate { _ :+
          Invocation(jobId, Thread.currentThread.getName, is(jobId).next(), result.get)
        }
        
        result.get
      } finally {
        invocations.foreach { invs =>
          for {
            (jid, ins) <- invs.groupBy(_.jobId)
          } {
            info(s"$jid:")
            ins.map(i => s"\t$i").foreach(info(_))
          }
          
          statusesByJobId.foreach(m => info(m.toString))
        }
        
        statusesByJobId.mutate { byJobId =>
          val newStatuses = byJobId(jobId).tail
        
          (byJobId - jobId) + (jobId -> newStatuses)
        }
      }
    }}

    override def shutdown(): Unit = isShutdown.update(true)
  }
}
