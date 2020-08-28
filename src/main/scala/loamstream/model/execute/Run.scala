package loamstream.model.execute

import java.time.LocalDateTime
import java.util.UUID

/**
 * @author clint
 * Aug 27, 2020
 */
final case class Run(name: String, time: LocalDateTime)

object Run {
  def create(name: String = UUID.randomUUID.toString): Run = Run(name, LocalDateTime.now) 
}
