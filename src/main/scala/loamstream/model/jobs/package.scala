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
  
  trait DrmJobOracle extends DirOracle[DrmTaskId] {
    def byJobIdMap: Map[String, Map[DrmTaskId, Path]]
  }
  
  final class DelegatingDrmJobOracle(private val delegate: DirOracle[DrmTaskId]) extends DrmJobOracle {
    override def dirOptFor(taskId: DrmTaskId): Option[Path] = delegate.dirOptFor(taskId)
  
    override def known: Set[DrmTaskId] = delegate.known
    
    override lazy val byJobIdMap: Map[String, Map[DrmTaskId, Path]] = {
      val byJobId: Map[String, Set[DrmTaskId]] = {
        val knownJobIds = known.map(_.jobId)
        
        knownJobIds.iterator.map { jobId =>
          jobId -> known.filter(_.jobId == jobId)
        }.toMap
      }
      
      byJobId.map {
        case (jobId, drmTaskIds) => jobId -> {
          drmTaskIds.iterator.map(taskId => taskId -> dirFor(taskId))
        }.toMap
      }
    }
  }
  
  object DrmJobOracle {
    def from(executionConfig: ExecutionConfig, executable: Executable, mapping: Map[DrmTaskId, LJob]): DrmJobOracle = {
      from(JobOracle.fromExecutable(executionConfig, executable), mapping)
    }
    
    def from(jobOracle: JobOracle, mapping: Map[DrmTaskId, LJob]): DrmJobOracle = from(jobOracle.via(mapping))
    
    private def from(delegate: DirOracle[DrmTaskId]): DrmJobOracle = new DelegatingDrmJobOracle(delegate) 
  }
}
