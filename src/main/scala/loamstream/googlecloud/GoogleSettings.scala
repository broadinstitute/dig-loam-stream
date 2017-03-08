package loamstream.googlecloud

import java.time.Instant

import loamstream.model.execute.Settings

/**
 * @author kyuksel
 *         date: 3/8/17
 */
final case class GoogleSettings(override val mem: Option[Float],
                                override val cpu: Option[Float],
                                override val startTime: Option[Instant],
                                override val endTime: Option[Instant],
                                override val node: Option[String]) extends Settings