package loamstream.drm

import java.nio.file.Path

/**
 * @author clint
 * Jun 6, 2018
 * 
 * Represents the information needed to run commands in a Singularity container. 
 */
final case class ContainerParams(imageName: String)
