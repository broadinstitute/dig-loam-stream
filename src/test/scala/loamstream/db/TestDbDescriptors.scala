package loamstream.db

import loamstream.db.slick.DbDescriptor
import _root_.slick.driver.H2Driver
import loamstream.util.Sequence

/**
 * @author clint
 * date: Aug 12, 2016
 */
object TestDbDescriptors {
  //TODO: This shouldn't be necessary :(
  private val sequence: Sequence[Int] = Sequence()
  
  //TODO: Unique DB names shouldn't be necessary :(
  def inMemoryH2 = DbDescriptor(H2Driver, s"jdbc:h2:mem:test${sequence.next()};DB_CLOSE_DELAY=-1", "org.h2.Driver")
}