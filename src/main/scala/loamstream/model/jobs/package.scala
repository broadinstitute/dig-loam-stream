package loamstream.model

import loamstream.util.DirTree
import loamstream.util.DirOracle
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.Executable
import loamstream.drm.DrmTaskId
import java.nio.file.Path

/**
 * @author clint
 * Sep 18, 2020
 */
package object jobs {
  type JobOracle = DirOracle[LJob]
  
  object JobOracle {
    def fromExecutable(executionConfig: ExecutionConfig, executable: Executable): DirOracle[LJob] = {
      new DirOracle.For(executionConfig, _.jobDataDir, executable.allJobs)
    }
  }
  
  trait DrmJobOracle extends DirOracle[DrmTaskId]
  
  final class DelegatingDrmJobOracle(private val delegate: DirOracle[DrmTaskId]) extends DrmJobOracle {
    override def dirOptFor(taskId: DrmTaskId): Option[Path] = delegate.dirOptFor(taskId)
 }
  
  object DrmJobOracle {
    def from(executionConfig: ExecutionConfig, executable: Executable, mapping: Map[DrmTaskId, LJob]): DrmJobOracle = {
      from(JobOracle.fromExecutable(executionConfig, executable), mapping)
    }
    
    def from(jobOracle: JobOracle, mappings: Iterable[(DrmTaskId, LJob)]): DrmJobOracle = {
      from(jobOracle.via(mappings.toMap))
    }
    
    private def from(delegate: DirOracle[DrmTaskId]): DrmJobOracle = new DelegatingDrmJobOracle(delegate) 
  }
}
