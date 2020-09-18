package loamstream.model

import loamstream.util.DirTree
import loamstream.util.DirOracle
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.Executable

/**
 * @author clint
 * Sep 18, 2020
 */
package object jobs {
  type JobOracle = DirOracle[LJob]
  
  object JobOracle {
    def fromExecutable(executionConfig: ExecutionConfig, executable: Executable): DirOracle[LJob] = {
      new DirOracle.For(executionConfig, executable.allJobs)
    }
  }
}
