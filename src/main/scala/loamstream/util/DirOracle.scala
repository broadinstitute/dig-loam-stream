package loamstream.util

import java.nio.file.Path
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.Executable
import loamstream.model.jobs.LJob

/**
 * @author clint
 * May 22, 2019
 */
trait DirOracle[A] {
  def dirOptFor(job: A): Option[Path]
  
  final def dirFor(job: A): Path = {
    val opt = dirOptFor(job)
    
    require(opt.isDefined, s"Value is not know to this oracle: $job")
    
    opt.get
  }
}

object DirOracle {
  final case class For[A : DirTree.DirNode.CanBeASimplePath](
      executionConfig: ExecutionConfig,
      getRootDir: ExecutionConfig => Path,
      values: Iterable[A]) extends DirOracle[A] {
    
    private val rootDir = getRootDir(executionConfig)
    
    private lazy val dirNode: DirTree.DirNode[A] = DirTree.allocate(values, executionConfig.maxJobLogFilesPerDir)
    
    private lazy val dirsByJob: Map[A, Path] = {
      dirNode.pathsByValue(rootDir)
    }
    
    private lazy val init: Unit = dirNode.makeDirsUnder(rootDir)

    private def initAndThen[A](body: => A): A = {
      def doInit(): Unit = init
      
      doInit()
        
      body
    }
    
    override def dirOptFor(job: A): Option[Path] = initAndThen {
      dirsByJob.get(job)
    }
  }
}
