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
    
    writeDataTo(makePath("failed-jobs.tsv"))(jobsToExecutions.filterValues(_.isFailure))
  }
  
  private def writeDataTo(file: Path)(executionTuples: Iterable[(LJob, Execution)]): Unit = {
    val csvFormat = CSVFormat.DEFAULT.
        withHeader("JOB_ID", "JOB_NAME", "JOB_STATUS", "EXIT_CODE", "JOB_DIR").
        withDelimiter('\t').
        withRecordSeparator(scala.util.Properties.lineSeparator)
    
    val toRow = Row.tupled
    
    val sorted = executionTuples.toSeq.sortWith(Orderings.executionTupleOrdering)
    
    CanBeClosed.enclosed(new CSVPrinter(new FileWriter(file.toFile), csvFormat)) { csvPrinter =>
      executionTuples.iterator.map(toRow).map(_.toJavaIterable).foreach(csvPrinter.printRecord)
    }
  }
  
  private final case class Row(job: LJob, ex: Execution) {
    def toJavaIterable: java.lang.Iterable[String] = {
      import scala.collection.JavaConverters._
      
      Iterable(job.id.toString, job.name, jobStatusPart(ex), exitCodePart(ex), jobDirPart(ex)).asJava
    }
    
    private def exitCodePart(e: Execution): String = e match {
      case Execution.WithCommandResult(cr) => cr.exitCode.toString
      case _ => "<not available>"
    }

    private def jobDirPart(e: Execution): String = e.jobDir.map(_.toAbsolutePath.toString).getOrElse("<not available>")

    private def jobStatusPart(e: Execution): String = e match {
      case Execution.WithThrowable(e) => s"Failed due to exception: '${e.getMessage}'"
      case _ => e.status.toString
    }
  }
}
