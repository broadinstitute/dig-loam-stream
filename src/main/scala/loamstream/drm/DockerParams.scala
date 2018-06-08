package loamstream.drm

import java.nio.file.Path

import loamstream.model.execute.Locations

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
 * NB: that this class does not expose `port_mapping` and `write_output`, or include them in YAML produced by toYaml, 
 * letting them be set to the defaults.
 * NB: This class does not expose `output_backend`, and always writes `output_hps_nobackup` as that field's value in
 * the YAML produced by toYaml.
 */
trait DockerParams extends Locations {
  def imageName: String
  
  def mountedDirs: Iterable[Path]
  
  def outputDir: Path
  
  override def inContainer(p: Path): Path = outputDir.resolve(p)
}
