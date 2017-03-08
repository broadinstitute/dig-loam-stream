package loamstream.model.execute

import java.time.Instant

/**
 * @author kyuksel
 *         date: 3/8/17
 */
final case class LocalSettings(override val mem: Option[Float],
                               override val cpu: Option[Float],
                               override val startTime: Option[Instant],
                               override val endTime: Option[Instant]) extends Settings
