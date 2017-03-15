package loamstream.model.execute

import loamstream.uger.Queue

/**
 * @author kyuksel
 *         date: 3/7/17
 */

sealed trait Settings

final case class UgerSettings(mem: Int,
                              cpu: Int,
                              queue: Queue) extends Settings

final case class GoogleSettings(cluster: String) extends Settings

final class LocalSettings extends Settings