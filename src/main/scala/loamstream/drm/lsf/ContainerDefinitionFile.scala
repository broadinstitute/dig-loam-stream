package loamstream.drm.lsf

import loamstream.drm.DockerParams
import org.yaml.snakeyaml.Yaml
import java.nio.file.Path
import loamstream.conf.LsfConfig
import loamstream.drm.DrmTaskArray
import loamstream.util.Files

/**
 * @author clint
 * Jun 12, 2018
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
 * NB: that this class does not expose `port_mapping` or include it in YAML produced by toYaml, letting it be set to 
 * the default.
 * NB: This class does not expose `output_backend`, and always writes `output_hps_nobackup` as that field's value in
 * the YAML produced by toYaml.  The same goes for `mount_home` and `write_output`, where `true` is used for both in
 * the YAML produced by toYaml.
 */
final case class ContainerDefinitionFile(dockerParams: LsfDockerParams, yamlFile: Path) {
  
  def writeYamlToFile(): Unit = Files.writeTo(yamlFile)(toYaml)
  
  def toYaml: String = {
    import java.{ util => ju }
    import loamstream.util.BashScript.Implicits._
    import scala.collection.JavaConverters._
    
    //NB: Have SnakeYaml serialize a java.util.Map, since this prevents it from writing a type-tag line starting 
    //with `!!`.  I'm not sure if that would break EBI's LSF, but better safe than sorry. -Clint June 6 2018
    //This also allows direct control over field names in the resulting YAML.
    val map: ju.Map[String, Any] = Map(
        "image" -> dockerParams.imageName,
        "mount_home" -> true,
        "mounts" -> dockerParams.mountedDirs.map(_.toAbsolutePath.render).toList.asJava,
        "write_output" -> true,
        "output" -> dockerParams.outputDir.toAbsolutePath.render,
        "output_backend" -> dockerParams.outputBackend.name).asJava
    
    (new Yaml).dump(map)
  }
}

object ContainerDefinitionFile {
  def apply(lsfConfig: LsfConfig, taskArray: DrmTaskArray, dockerParams: LsfDockerParams): ContainerDefinitionFile = {
    val yamlFileName = s"${taskArray.drmJobName}.yaml"
    
    val yamlFilePath = lsfConfig.workDir.resolve(yamlFileName)
    
    ContainerDefinitionFile(dockerParams, yamlFilePath)
  }
}
