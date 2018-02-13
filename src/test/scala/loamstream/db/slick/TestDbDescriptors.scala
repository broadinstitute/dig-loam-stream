package loamstream.db.slick

import loamstream.util.Sequence
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.Path

/**
 * @author clint
 * date: Aug 12, 2016
 */
object TestDbDescriptors {
  @deprecated("", "")
  def inMemoryH2: DbDescriptor = DbDescriptor.inMemory
}
