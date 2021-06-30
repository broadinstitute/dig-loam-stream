package loamstream.apps

import java.io.FileWriter
import java.nio.file.Path

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter

import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.CanBeClosed
import loamstream.util.Files
import loamstream.model.jobs.JobStatus
import java.time.LocalDateTime
import scala.collection.compat._

/**
 * @author clint
 * Jun 4, 2019
 */
object IndexFiles {
  def writeIndexFiles(
    executionConfig: ExecutionConfig,
    jobsToExecutions: Map[LJob, Execution]): Unit = {

    def makePath(fileName: String) = executionConfig.logDir.resolve(fileName)

    Files.createDirsIfNecessary(executionConfig.logDir)
    
    writeDataTo(makePath("all-jobs.tsv"))(jobsToExecutions)

    import loamstream.util.Maps.Implicits._

    def shouldGoToFailureFile(execution: Execution): Boolean = execution.isFailure || execution.status.isCanceled
    
    writeDataTo(makePath("failed-jobs.tsv"))(jobsToExecutions.filterValues(shouldGoToFailureFile))
  }
  
  private def writeRowsTo(file: Path)(rows: Iterable[Row]): Unit = {
    val sorted = rows.to(Seq).sortWith(Orderings.indexFileRowOrdering)
    
    CanBeClosed.using(new CSVPrinter(new FileWriter(file.toFile), Row.csvFormat)) { csvPrinter =>
      sorted.iterator.map(_.toJavaIterable).foreach(csvPrinter.printRecord)
    }
  }
  
  private def writeDataTo(file: Path)(executionTuples: Iterable[(LJob, Execution)]): Unit = {
    val toRow: (LJob, Execution) => Row = Row(_, _)
    
    writeRowsTo(file)(executionTuples.map(toRow.tupled))
  }
  
  final case class Row(
      id: String, 
      name: String, 
      exitCode: Option[Int], 
      jobDir: Option[Path],
      jobStatus: JobStatus, 
      failureCause: Option[Throwable],
      startTime: Option[LocalDateTime]) {
    def toJavaIterable: java.lang.Iterable[String] = {
      import scala.collection.JavaConverters._
      
      Iterable(id, name, jobStatusPart, exitCodePart, jobDirPart).asJava
    }
    
    private def exitCodePart: String = exitCode match {
      case Some(ec) => ec.toString
      case _ => "<not available>"
    }

    private def jobDirPart: String = jobDir.map(_.toAbsolutePath.toString).getOrElse("<not available>")

    private def jobStatusPart: String = failureCause match {
      case Some(e) => s"Failed due to exception: '${e.getMessage}'"
      case _ => jobStatus.toString
    }
  }
  
  object Row {
    val csvFormat: CSVFormat = CSVFormat.DEFAULT.
        withHeader("JOB_ID", "JOB_NAME", "JOB_STATUS", "EXIT_CODE", "JOB_DIR").
        withDelimiter('\t').
        withRecordSeparator(scala.util.Properties.lineSeparator)
    
    def apply(tuple: (LJob, Execution)): Row = apply(tuple._1, tuple._2) 
    
    def apply(job: LJob, ex: Execution): Row = {
      val exitCode: Option[Int] = ex match {
        case Execution.WithCommandResult(cr) => Option(cr.exitCode)
        case _ => None
      }
      
      val failureCause: Option[Throwable] = ex match {
        case Execution.WithThrowable(e) => Option(e)
        case _ => None
      }
      
      new Row(job.id.toString, job.name, exitCode, ex.jobDir, ex.status, failureCause, ex.resources.map(_.startTime))
    }
  }
}
