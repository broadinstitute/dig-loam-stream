package loamstream.apps

import java.nio.file.Path

import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Files
import loamstream.model.jobs.JobResult
import loamstream.util.MissingFileTimeoutException
import loamstream.util.Classes
import loamstream.model.jobs.JobStatus

/**
 * @author clint
 * Jun 4, 2019
 */
object IndexFiles {
  def writeIndexFiles(
    executionConfig: ExecutionConfig,
    jobsToExecutions: Map[LJob, Execution]): Unit = {

    def tabSeperate[A](as: A*): String = as.mkString("\t")

    val headerLine = tabSeperate("JOB_ID", "JOB_NAME", "JOB_STATUS", "EXIT_CODE", "JOB_DIR")

    def write(tuples: Iterable[(LJob, Execution)], dest: Path): Unit = {
      val sorted = tuples.toSeq.sortWith(Orderings.executionTupleOrdering)

      def exitCodePart(e: Execution): String = e match {
        case Execution.WithCommandResult(cr) => cr.exitCode.toString
        case _ => "<not available>"
      }

      def jobDirPart(e: Execution): String = e.jobDir.map(_.toAbsolutePath.toString).getOrElse("<not available>")

      def jobStatusPart(e: Execution): String = e match {
        case Execution.WithThrowable(e) => s"Failed due to exception: '${e.getMessage}'"
        case _ => e.status.toString
      }
      
      val lines = sorted.map {
        case (j, e) => tabSeperate(j.id, j.name, jobStatusPart(e), exitCodePart(e), jobDirPart(e))
      }

      val contents = (headerLine +: lines).mkString(System.lineSeparator)

      Files.writeTo(dest)(contents)
    }

    import loamstream.util.Maps.Implicits._

    def makePath(fileName: String) = executionConfig.logDir.resolve(fileName)

    Files.createDirsIfNecessary(executionConfig.logDir)
    
    write(jobsToExecutions, makePath("all-jobs.tsv"))

    write(jobsToExecutions.filterValues(_.isFailure), makePath("failed-jobs.tsv"))
  }
}
