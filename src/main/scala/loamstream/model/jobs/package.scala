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
  
  type DrmJobOracle = DirOracle[DrmTaskId]
  
  object JobOracle {
    def fromExecutable(executionConfig: ExecutionConfig, executable: Executable): DirOracle[LJob] = {
      new DirOracle.For(executionConfig, _.jobDataDir, executable.allJobs)
    }
  }
  
  object DrmJobOracle {
    def from(executionConfig: ExecutionConfig, executable: Executable, mapping: Map[DrmTaskId, LJob]): DrmJobOracle = {
      JobOracle.fromExecutable(executionConfig, executable).via(mapping)
    }
  }
}
