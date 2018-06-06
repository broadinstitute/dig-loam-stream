package loamstream.drm.lsf

import java.nio.file.Path
import java.nio.file.Paths

import scala.beans.BeanProperty

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.DumperOptions

/**
 * @author clint
 * Jun 6, 2018
 * 
 * Represents data in a Container Definition File, as described in the docker-hpc presentation slides shared by EBI.
 * (Unfortunately, we don't have access to any documentation beyond that.)
 *
 * Required YAML fields:
 * `image`: Docker image name (String, for example `library/ubuntu:18.04`)
 * 
 * Optional YAML fields:
 * `mount_homes`: Whether to mount (read-only) the user's Home directory or not (Boolean, default: false)
 * `mounts`: Directories to mount from the physical host (Sequence of Strings, default: empty list)
 * `write_output`: Whether container needs to write data or not (Boolean, default: false)
 * `output`: Output data mountpoint (needs write_output to be true) (Path string, default: `/output`)
 * `output_backend`: Filesystem to be used to write data Specific per cluster (if write_output is true) (String,
 *   default: unknown, cluster-defined; one possible value is `output_hps_nobackup`, which (probably) means to write
 *   logs and mount outputs under /hps/nobackup/docker/<username>/
 * `port_mapping` Map a specific port within the container (Int, default: no mapped ports)
 * 
 * NB: that this class does not expose `port_mapping` or include it in YAML produced by toYaml, letting that be set 
 * to the default.
 * NB: This class does not expose `output_backend`, and always writes `output_hps_nobackup` as that field's value in
 * the YAML produced by toYaml.
 */
final case class ContainerDefinitionFile(
    image: String,
    mountHomes: Boolean = false,
    mounts: Iterable[Path] = Nil,
    writeOutput: Boolean = false,
    output: Path = Paths.get("/output")) {
  
  def toYaml: String = ContainerDefinitionFile.toYaml(this)
}

object ContainerDefinitionFile {
  sealed abstract class OutputBackend private (val name: String)
  
  object OutputBackend {
    case object OutputHpsNobackup extends OutputBackend("output_hps_nobackup")
    
    def default: OutputBackend = OutputHpsNobackup 
  }
  
  private lazy val defaultDumperOptions: DumperOptions = {
    val options = new DumperOptions
    
    options.setAllowReadOnlyProperties(true)
    
    options
  }

  import java.{ util => ju }
  
  def toYaml(cdf: ContainerDefinitionFile): String = {
    import loamstream.util.BashScript.Implicits._
    import scala.collection.JavaConverters._
    
    //NB: Have SnakeYaml serialize a java.util.Map, since this prevents it from writing a type-tag line starting 
    //with `!!`.  I'm not sure if that would break EBI's LSF, but better safe than sorry. -Clint June 6 2018
    //This also allows direct control over field names in the resulting YAML.
    val map: ju.Map[String, Any] = Map(
        "image" -> cdf.image,
        "mount_homes" -> cdf.mountHomes,
        "mounts" -> cdf.mounts.map(_.render).toList.asJava,
        "write_output" -> cdf.writeOutput,
        "output" -> cdf.output.render,
        "output_backend" -> OutputBackend.default.name).asJava
    
    (new Yaml(defaultDumperOptions)).dump(map)
  }
}

 
