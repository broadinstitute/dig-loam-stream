package loamstream.drm

import org.scalatest.FunSuite
import loamstream.model.execute.DrmSettings
import DrmSubmissionResult.SubmissionSuccess
import DrmSubmissionResult.SubmissionFailure
import loamstream.util.Tries
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobStatus
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.DrmSettings
import loamstream.conf.UgerConfig
import loamstream.drm.uger.UgerPathBuilder
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.TestHelpers
import rx.lang.scala.Observable
import loamstream.util.Observables

/**
 * @author clint
 * Jul 8, 2020
 */
final class JobSubmitterTest extends FunSuite {
  import JobSubmitterTest.MockJobSubmitter
  import TestHelpers.{path, waitFor}
  import Observables.Implicits.ObservableOps
  
  private val ugerConfig = UgerConfig() 
  private val settings = DrmSettings.fromUgerConfig(ugerConfig)
  private val job = CommandLineJob(commandLineString = "foo --bar", initialSettings = settings)
  private val pathBuilder = UgerPathBuilder(UgerScriptBuilderParams(ugerConfig))
    
  private val wrapper = DrmJobWrapper(
    executionConfig = ExecutionConfig.default,
    drmSettings = settings,
    pathBuilder = pathBuilder,
    commandLineJob = job,
    jobDir = path("foo"),
    drmIndex = 1)
  
  private val drmId = DrmTaskId("fooJob", 1)
    
  test("Retrying - no retries, submission succeeds") {
    val expectedMapping = Map(drmId -> wrapper)
    
    val submitter = JobSubmitter.Retrying(MockJobSubmitter(expectedMapping, 0), maxRetries = 2)
    
    val taskArray = DrmTaskArray.fromCommandLineJobs(
        ExecutionConfig.default, 
        TestHelpers.DummyJobOracle, 
        settings, 
        ugerConfig, 
        pathBuilder, 
        Seq(job), 
        "fooArray")
    
    val result = waitFor(submitter.submitJobs(settings, taskArray).firstAsFuture)
    
    assert(result === SubmissionSuccess(expectedMapping))
  }
  
  test("Retrying - no retries, submission fails") {
    val expectedMapping = Map(drmId -> wrapper)
    
    val submitter = JobSubmitter.Retrying(MockJobSubmitter(expectedMapping, 0), maxRetries = 2)
    
    val taskArray = DrmTaskArray.fromCommandLineJobs(
        ExecutionConfig.default, 
        TestHelpers.DummyJobOracle, 
        settings, 
        ugerConfig, 
        pathBuilder, 
        Seq(job), 
        "fooArray")
    
    val result = waitFor(submitter.submitJobs(settings, taskArray).firstAsFuture)
    
    assert(result === SubmissionSuccess(expectedMapping))
  }
}

object JobSubmitterTest {
  private final case class MockJobSubmitter(
      toReturn: Map[DrmTaskId, DrmJobWrapper],
      initialFailures: Int) extends JobSubmitter {
    
    private var timesSubmitInvoked: Int = 0 
    
    override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
      Observable.just {
        timesSubmitInvoked += 1
        
        if(timesSubmitInvoked > initialFailures) {
          SubmissionSuccess(toReturn)
        } else {
          Tries.failure("submission failure")
        }
      }
    }
    
    override def stop(): Unit = ???
  }
}
