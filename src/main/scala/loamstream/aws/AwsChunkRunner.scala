package loamstream.aws

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import loamstream.loam.aws.AwsApi
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.EnvironmentType
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.model.jobs.aws.AwsJob
import rx.lang.scala.Observable
import loamstream.model.execute.AwsSettings
import loamstream.util.Observables

/**
 * @author clint
 * Oct 21, 2019
 */
final class AwsChunkRunner(awsApi: AwsApi) extends ChunkRunnerFor(EnvironmentType.Aws) {
  override def maxNumJobs: Int = 1000 //TODO

  override def run(
    jobs: Set[LJob],
    jobOracle: JobOracle,
    shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {

    if(jobs.isEmpty) {
      Observable.just(Map.empty)
    } else {
      require(
          jobs.forall(_.initialSettings.envType.isAws), 
          s"Only AWS jobs are supported by this ${getClass.getSimpleName}")
      
      val awsJobs: Iterable[AwsJob] = jobs.collect { case aj: AwsJob => aj }
  
      val subChunkStreams = groupBySettings(awsJobs).toSeq.map(runSubChunk(jobOracle))
      
      Observables.merge(subChunkStreams)
    }
  }
  
  private def runSubChunk(jobOracle: JobOracle)(tuple: (AwsSettings, Iterable[AwsJob])): Observable[Map[LJob, RunData]] = {
    val z: Map[LJob, RunData] = Map.empty
    
    val (awsSettings, awsJobs) = tuple
    
    Observable.from(awsJobs).map(runSingleJob(jobOracle)).scan(z)(_ + _)
  }
  
  private def groupBySettings(jobs: Iterable[AwsJob]): Map[AwsSettings, Seq[AwsJob]] = {
    import loamstream.util.Maps.Implicits._
    
    jobs.groupBy(_.initialSettings).collectKeys { 
      case awsSettings: AwsSettings => awsSettings
    }.mapValues(_.toSeq)
  }

  private def runSingleJob(jobOracle: JobOracle)(j: AwsJob): (LJob, RunData) = {
    def run(): Try[Any] = {
      println(s"%%%%%%%%% Running '${j}'")
      
      Try(j.body(awsApi))
    }
    
    val (status, result) = run() match {
      case Success(_) => (JobStatus.Succeeded, Some(JobResult.Success))
      case Failure(e) => (JobStatus.Succeeded, Some(JobResult.FailureWithException(e)))
    }

    j -> RunData(
      job = j,
      settings = j.initialSettings,
      jobStatus = status,
      jobResult = result,
      resourcesOpt = None,
      jobDirOpt = jobOracle.dirOptFor(j),
      terminationReasonOpt = None)
  }
}
