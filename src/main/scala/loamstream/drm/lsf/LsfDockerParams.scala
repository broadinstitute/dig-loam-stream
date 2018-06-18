package loamstream.drm.lsf

import java.nio.file.Path
import java.nio.file.Paths.{ get => path }

import LsfDockerParams.OutputBackend
import loamstream.drm.DockerParams
import loamstream.util.PathEnrichments

/**
 * @author clint
 * Jun 6, 2018
 */
final case class LsfDockerParams(
    imageName: String,
    mountedDirs: Iterable[Path],
    outputDir: Path,
    outputBackend: OutputBackend = OutputBackend.default) extends DockerParams {
  
  require(outputDir.isAbsolute, s"Output dir (in container) must be absolute, but got '${outputDir}'")
  
  import LsfDockerParams.append
  
  override def inContainer(p: Path): Path = append(outputDir, p)
  
  override def inHost(p: Path): Path = append(outputBackend.basePath, p)
}

object LsfDockerParams {

  private[lsf] def append(base: Path, rest: Path): Path = {
    def toRelative(path: Path): Path = path.subpath(0, path.getNameCount)
    
    val toAppend = toRelative(rest)
    
    base.resolve(toAppend)
  }
  
  sealed abstract class OutputBackend private (val name: String) {
    def basePath: Path
  }
  
  object OutputBackend {
    case object OutputHpsNobackup extends OutputBackend("output_hps_nobackup") {
      override val basePath: Path = {
        val username = System.getProperty("user.name")
        
        import PathEnrichments._
        
        path("/hps/nobackup/docker") / username / "output"
      }
    }
    
    def default: OutputBackend = OutputHpsNobackup 
  }
}
