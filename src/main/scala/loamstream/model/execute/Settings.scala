package loamstream.model.execute

import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/7/17
 */

sealed trait Settings

//TODO: Can we require an amount of ram for local jobs?  LocalSettings should be empty if we can't. 
final case class LocalSettings() extends Settings

final case class UgerSettings(mem: Int,
                              cpu: Int,
                              queue: Queue) extends Settings

final case class GoogleSettings(cluster: String) extends Settings
