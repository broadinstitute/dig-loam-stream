package loamstream.model.execute

import java.time.Instant

/**
 * @author kyuksel
 *         date: 3/8/17
 */
final case class LocalSettings(override val memReq: Option[Float],
                               override val memAct: Option[Float],
                               override val cpuReq: Option[Float],
                               override val cpuAct: Option[Float],
                               override val startTime: Option[Instant],
                               override val endTime: Option[Instant]) extends Settings
