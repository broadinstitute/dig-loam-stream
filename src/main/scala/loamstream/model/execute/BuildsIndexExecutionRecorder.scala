package loamstream.model.execute

import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Execution
import java.nio.file.Path
import loamstream.util.ValueBox
import loamstream.model.jobs.JobStatus
import loamstream.util.Files
import BuildsIndexExecutionRecorder.RunRecord
import BuildsIndexExecutionRecorder.Index

/**
 * @author clint
 * May 29, 2019
 */
final class BuildsIndexExecutionRecorder extends ExecutionRecorder {

  private[this] val recordsBox: ValueBox[Seq[RunRecord]] = ValueBox(Vector.empty)
  
  override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
    val records = executionTuples.map {
      case (j, e) => RunRecord(j, jobOracle.dirFor(j), e)
    }
    
    recordsBox.mutate(_ ++ records)
  }
  
  def records: Seq[RunRecord] = recordsBox()
  
  def toIndex: Index = Index(records)
}

object BuildsIndexExecutionRecorder {
  final case class RunRecord(job: LJob, jobDir: Path, execution: Execution) {
    def jobId: Int = job.id
    
    def jobName: String = job.name
    
    def jobStatus: JobStatus = execution.status
  }
  
  final case class Index(records: Seq[RunRecord]) {
    def filter(p: RunRecord => Boolean): Index = copy(records = records.filter(p))
    
    def writeToFile(file: Path): Unit = {
      val headerLine = "JOB_ID\tJOB_NAME\tJOB_STATUS\tJOB_DIR"
      
      def toLine(r: RunRecord) = s"${r.jobId}\t${r.jobName}\t${r.jobStatus}\t${r.jobDir}"
      
      val contents = (headerLine +: records.map(toLine)).mkString("\n")
      
      Files.writeTo(file)(contents)
    }
  }
}
