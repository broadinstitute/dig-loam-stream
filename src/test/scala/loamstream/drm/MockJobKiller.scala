package loamstream.drm

/**
 * @author clint
 * Jul 15, 2020
 */
object MockJobKiller {
  object DoesNothing extends JobKiller {
    override protected def doKillAllJobs(): Unit = ()
  }
}
