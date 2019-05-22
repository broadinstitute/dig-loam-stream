package loamstream.model.jobs

import java.nio.file.Path
import loamstream.conf.ExecutionConfig

/**
 * @author clint
 * May 22, 2019
 */
trait JobOracle {
  def logDirFor(job: LJob): Option[Path]
}

object JobOracle {
  final class ForJobs(executionConfig: ExecutionConfig, jobs: Iterable[LJob]) extends JobOracle {
    private lazy val dirNode: JobDirs.DirNode = JobDirs.allocate(jobs, ???)
    
    private lazy val dirsByJob: Map[LJob, Path] = dirNode.pathsByJob
    
    private lazy val init: Unit = dirNode.makeDirsUnder(executionConfig.jobDir)

    private def initAndThen[A](body: => A): A = {
      def doInit(): Unit = init
      
      doInit()
        
      body
    }
    
    override def logDirFor(job: LJob): Option[Path] = initAndThen {
      dirsByJob.get(job)
    }
  }
}
