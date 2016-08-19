package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.model.jobs.Output.CachedOutput
import loamstream.util.Hash
import loamstream.util.HashType
import loamstream.util.Hashes


/**
 * @author clint
 * date: Aug 9, 2016
 */
final class SlickLoamDaoTest extends FunSuite with ProvidesSlickLoamDao {
  
  override val descriptor = TestDbDescriptors.inMemoryH2
  
  test("insert/Read") {
    createTablesAndThen {
      val path = Paths.get("src/test/resources/for-hashing/foo.txt")

      dao.storeHash(path, Hashes.sha1(path))

      val hash = dao.hashFor(path)

      assert(hash == Hashes.sha1(path))
    }
  }

  private def cachedOutput(p: Path, h: Hash): CachedOutput = (new RawOutputRow(p, h)).toCachedOutput
  
  test("insert/allRows") {
    createTablesAndThen {
      val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val path1 = Paths.get("src/test/resources/for-hashing/bigger")
      val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allRows.isEmpty)

      val hash0 = Hashes.sha1(path0)
      val hash1 = Hashes.sha1(path1)
      val hash2 = Hashes.sha1(path2)

      dao.storeHash(path0, hash0)
      dao.storeHash(path1, hash1)
      dao.storeHash(path2, hash2)

      val expected = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1), cachedOutput(path2, hash2))

      //Use Sets to ignore order
      assert(dao.allRows.toSet == expected)
    }
  }

  test("delete all") {
    createTablesAndThen {
      val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val path1 = Paths.get("src/test/resources/for-hashing/bigger")
      val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allRows.isEmpty)

      val hash0 = Hashes.sha1(path0)
      val hash1 = Hashes.sha1(path1)
      val hash2 = Hashes.sha1(path2)

      dao.storeHash(path0, hash0)
      dao.storeHash(path1, hash1)
      dao.storeHash(path2, hash2)

      assert(dao.allRows.size == 3)

      dao.delete(path0, path1, path2)

      assert(dao.allRows.isEmpty)
    }
  }
  
  test("delete some") {
    createTablesAndThen {
      val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val path1 = Paths.get("src/test/resources/for-hashing/bigger")
      val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allRows.isEmpty)

      val hash0 = Hashes.sha1(path0)
      val hash1 = Hashes.sha1(path1)
      val hash2 = Hashes.sha1(path2)

      dao.storeHash(path0, hash0)
      dao.storeHash(path1, hash1)
      dao.storeHash(path2, hash2)

      assert(dao.allRows.size == 3)

      dao.delete(path0, path2)

      assert(dao.allRows.map(_.path) == Seq(path1.toAbsolutePath))
      
      dao.delete(path0, path1)
      
      assert(dao.allRows.isEmpty)
    }
  }
  
  test("insertOrUpdate") {
    createTablesAndThen {
      val p0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val p1 = Paths.get("src/test/resources/for-hashing/bigger")
      val p2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allRows.isEmpty)

      val h0 = Hashes.sha1(p0)
      val h1 = Hashes.sha1(p1)
      val h2 = Hashes.sha1(p2)
      
      val rows = Seq(
          cachedOutput(p0, h0),
          cachedOutput(p1, h1),
          cachedOutput(p2, h2))

      dao.insertOrUpdate(rows)
          
      assert(dao.allRows.toSet == rows.toSet)
      
      val h0prime = Hash(Array(0.toByte), HashType.Sha1)
      val h1prime = Hash(Array(1.toByte), HashType.Sha1)
      
      val updatedRows = Seq(
          cachedOutput(p0, h0prime),
          cachedOutput(p1, h1prime))
          
      dao.insertOrUpdate(updatedRows)
      
      val expected = updatedRows :+ cachedOutput(p2, h2)

      assert(dao.allRows.toSet == expected.toSet)
    }
  }
}