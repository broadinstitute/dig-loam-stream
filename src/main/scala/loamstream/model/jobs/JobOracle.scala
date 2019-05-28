package loamstream.model.jobs

import java.nio.file.Path
import loamstream.conf.ExecutionConfig

/**
 * @author clint
 * May 22, 2019
 */
trait JobOracle {
  def dirOptFor(job: LJob): Option[Path]
  
  final def dirFor(job: LJob): Path = {
    val opt = dirOptFor(job)
    
    require(opt.isDefined, s"Job is not know to this oracle: $job")
    
    opt.get
  }
}

object JobOracle {
  final class ForJobs(executionConfig: ExecutionConfig, jobs: Iterable[LJob]) extends JobOracle {
    private lazy val dirNode: JobDirs.DirNode = JobDirs.allocate(jobs, executionConfig.maxJobLogFilesPerDir)
    
    private lazy val dirsByJob: Map[LJob, Path] = dirNode.pathsByJob(executionConfig.jobDataDir)
    
    private lazy val init: Unit = dirNode.makeDirsUnder(executionConfig.jobDataDir)

    private def initAndThen[A](body: => A): A = {
      def doInit(): Unit = init
      
      doInit()
        
      body
    }
    
    override def dirOptFor(job: LJob): Option[Path] = initAndThen {
      dirsByJob.get(job)
    }
  }
}
