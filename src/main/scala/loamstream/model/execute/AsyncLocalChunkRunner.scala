package loamstream.model.execute

import AsyncLocalChunkRunner.defaultMaxNumJobs
import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.ProcessLoggers
import loamstream.util.ThisMachine
import loamstream.util.Throwables
import monix.eval.Task
import monix.reactive.Observable
import scala.collection.compat._


/**
 * @author clint
 * Nov 22, 2016
 */
final case class AsyncLocalChunkRunner(
    executionConfig: ExecutionConfig,
    maxNumJobs: Int = defaultMaxNumJobs) extends ChunkRunnerFor(EnvironmentType.Local) {

  import AsyncLocalChunkRunner._
  
  override def run(
      jobs: Iterable[LJob], 
      jobOracle: JobOracle): Observable[(LJob, RunData)] = {
    
    if(jobs.isEmpty) { Observable.empty }
    else {
      import LocalJobStrategy.canBeRun
      
      require(
          jobs.forall(canBeRun), 
          s"Expected only LocalJobs, but found ${jobs.filterNot(canBeRun).mkString(",")}")
      
      def exec(job: LJob): Observable[RunData] = {
        Observable.from(executeSingle(executionConfig, jobOracle, job))
      }

      val executionObservables: Seq[Observable[RunData]] = jobs.to(Seq).map(exec)
        
      Observables.merge(executionObservables).map(runData => (runData.job -> runData))
    }
  }
}

object AsyncLocalChunkRunner extends Loggable {
  def defaultMaxNumJobs: Int = ThisMachine.numCpus
  
  def executeSingle(
      executionConfig: ExecutionConfig,
      jobOracle: JobOracle,
      job: LJob): Task[RunData] = {
    
    val jobDir = jobOracle.dirFor(job)
    
    val processLogger = ProcessLoggers.toFilesInDir(jobDir)
    
    val result = LocalJobStrategy.execute(job, jobDir, processLogger)

  
    def closeProcessLogger: Task[Unit] = Task {
      Throwables.quietly("Closing process logger failed")(processLogger.close())
    }
    
    for {
      r <- result 
      _ <- closeProcessLogger
    } yield r
  }
}
