package loamstream.uger

import java.time.Instant

import loamstream.model.execute.Settings

/**
 * @author kyuksel
 *         date: 3/7/17
 */
final case class UgerSettings(override val memReq: Option[Float],
                              override val memAct: Option[Float],
                              override val cpuReq: Option[Float],
                              override val cpuAct: Option[Float],
                              override val startTime: Option[Instant],
                              override val endTime: Option[Instant],
                              override val node: Option[String],
                              override val queue: Option[Queue]) extends Settings
