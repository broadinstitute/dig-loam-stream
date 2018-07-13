package loamstream.drm

import java.nio.file.Path

/**
 * @author clint
 * Jun 6, 2018
 * 
 * Represents the information needed to run commands in a Docker container. 
 */
trait DockerParams {
  def imageName: String
}
