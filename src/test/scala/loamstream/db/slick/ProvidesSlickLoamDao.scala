package loamstream.db.slick

import java.nio.file.Path
import java.time.Instant

import scala.util.Try

import loamstream.model.jobs.Execution
import loamstream.model.jobs.StoreRecord
import loamstream.util.Hash
import loamstream.util.Paths
import scala.concurrent.Await
import loamstream.TestHelpers
import org.scalactic.Equality

/**
 * @author clint
 * date: Aug 12, 2016
 */
trait ProvidesSlickLoamDao extends TestDbOps with ExecutionEquality.Implicits  with TestDbHelpers {
  
  protected val descriptor: DbDescriptor = DbDescriptor.inMemory
  
  protected override lazy val dao: SlickLoamDao = new SlickLoamDao(descriptor)
}
