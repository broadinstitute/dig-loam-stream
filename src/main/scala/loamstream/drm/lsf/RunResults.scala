package loamstream.drm.lsf

/**
 * @author clint
 * May 15, 2018
 */
final case class RunResults(executable: String, exitCode: Int, stdout: Seq[String], stderr: Seq[String])
