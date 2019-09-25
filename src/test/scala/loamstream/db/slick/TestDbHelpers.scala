package loamstream.db.slick

import java.nio.file.Path
import java.time.Instant

import loamstream.model.jobs.StoreRecord
import loamstream.util.Hash
import loamstream.util.Paths

/**
 * @author clint
 * Sep 12, 2019
 */
object TestDbHelpers extends TestDbHelpers

trait TestDbHelpers {
  protected def cachedOutput(path: Path, hash: Hash, lastModified: Instant): StoreRecord = {
    val hashValue = hash.valueAsBase64String

    StoreRecord(
        Paths.normalize(path), 
        () => Option(hashValue), 
        () => Option(hash.tpe.algorithmName), 
        Option(lastModified))
  }

  protected def cachedOutput(path: Path, hash: Hash): StoreRecord = {
    cachedOutput(path, hash, Instant.ofEpochMilli(0))
  }

  protected def failedOutput(path: Path): StoreRecord = StoreRecord(Paths.normalize(path))
}
