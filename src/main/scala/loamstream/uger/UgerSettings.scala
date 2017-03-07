package loamstream.uger

import java.time.Instant

import loamstream.model.execute.Settings

/**
 * @author kyuksel
 *         date: 3/7/17
 */
final case class UgerSettings(override val mem: Option[Float],
                              override val cpu: Option[Float],
                              override val startTime: Option[Instant],
                              override val endTime: Option[Instant],
                              override val node: Option[String],
                              override val queue: Option[Queue]) extends Settings
