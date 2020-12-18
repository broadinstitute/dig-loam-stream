package loamstream.model.execute

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.util.Terminable
import loamstream.util.ValueBox

/**
 * @author clint
 * Dec 18, 2020
 *
 * Writes output locations of successful jobs to the specified file
 */
final case class SuccessfulOutputsExecutionRecorder(file: Path) extends ExecutionRecorder with Terminable {
  private lazy val writer: PrintWriter = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8))
  
  private[this] val anythingWritten: ValueBox[Boolean] = ValueBox(false)
  
  override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
    executionTuples.foreach { case (j, e) =>
      if(e.isSuccess && e.outputs.nonEmpty) {
        try {
          e.outputs.iterator.map(_.loc).foreach(writer.println)
        } finally {
          //Flush after writing each batch.  This will reduce performance, but will make it easier to tail
          //the resulting file over the course of a long run.
          writer.flush()
        }
      }
    }
  }
  
  override def stop(): Unit = {
    if(anythingWritten()) {
      writer.close()
    }
  }
}
