package loamstream.model.execute

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobOracle
import java.nio.file.Path
import loamstream.util.Terminable
import loamstream.util.Loggable
import loamstream.util.Throwables
import loamstream.util.LogContext
import org.apache.commons.csv.CSVFormat
import loamstream.loam.intake.RowSink
import java.nio.file.Files
import java.io.Writer
import loamstream.util.ValueBox
import java.nio.charset.StandardCharsets
import java.io.PrintWriter

/**
 * @author clint
 * Jul 2, 2018
 */
trait ExecutionRecorder extends Terminable {
  def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit
  
  final def &&(rhs: ExecutionRecorder): ExecutionRecorder = ExecutionRecorder.CompositeExecutionRecorder(this, rhs)
  
  override def stop(): Unit = ()
}

object ExecutionRecorder {
  final case class CompositeExecutionRecorder(
      a: ExecutionRecorder, 
      b: ExecutionRecorder) extends ExecutionRecorder with Loggable {
    
    override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = {
      a.record(jobOracle, executionTuples)
      
      b.record(jobOracle, executionTuples)
    }
    
    override def stop(): Unit = {
      def doStop(er: ExecutionRecorder): Unit = {
        Throwables.quietly("Error shutting down ExecutionRecorder $er: ", LogContext.Level.Warn) {
          er.stop()
        }
      }
      
      doStop(a)
      doStop(b)
    }
  }
  
  object DontRecord extends ExecutionRecorder {
    override def record(jobOracle: JobOracle, executionTuples: Iterable[(LJob, Execution)]): Unit = ()
  }
  
  /**
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
}
