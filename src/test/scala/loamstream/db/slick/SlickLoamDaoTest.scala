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
  
  test("insert/Read Outputs") {
    createTablesAndThen {
      val path = Paths.get("src/test/resources/for-hashing/foo.txt")

      store(path, Hashes.sha1(path))

      val Seq(row) = dao.allOutputRows
      
      assert(row.hash == Hashes.sha1(path))
    }
  }

  test("insert/allOutputRows") {
    createTablesAndThen {
      val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val path1 = Paths.get("src/test/resources/for-hashing/bigger")
      val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allOutputRows.isEmpty)

      val hash0 = Hashes.sha1(path0)
      val hash1 = Hashes.sha1(path1)
      val hash2 = Hashes.sha1(path2)

      store(path0, hash0)
      store(path1, hash1)
      store(path2, hash2)

      val expected = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1), cachedOutput(path2, hash2))

      //Use Sets to ignore order
      assert(dao.allOutputRows.toSet == expected)
    }
  }

  test("delete all Outputs") {
    createTablesAndThen {
      val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val path1 = Paths.get("src/test/resources/for-hashing/bigger")
      val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allOutputRows.isEmpty)

      val hash0 = Hashes.sha1(path0)
      val hash1 = Hashes.sha1(path1)
      val hash2 = Hashes.sha1(path2)

      store(path0, hash0)
      store(path1, hash1)
      store(path2, hash2)

      assert(dao.allOutputRows.size == 3)

      dao.deleteOutput(path0, path1, path2)

      assert(dao.allOutputRows.isEmpty)
    }
  }
  
  test("delete some Outputs") {
    createTablesAndThen {
      val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val path1 = Paths.get("src/test/resources/for-hashing/bigger")
      val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allOutputRows.isEmpty)

      val hash0 = Hashes.sha1(path0)
      val hash1 = Hashes.sha1(path1)
      val hash2 = Hashes.sha1(path2)

      store(path0, hash0)
      store(path1, hash1)
      store(path2, hash2)

      assert(dao.allOutputRows.size == 3)

      dao.deleteOutput(path0, path2)

      assert(dao.allOutputRows.map(_.path) == Seq(path1.toAbsolutePath))
      
      dao.deleteOutput(path0, path1)
      
      assert(dao.allOutputRows.isEmpty)
    }
  }
  
  test("insertOrUpdateOutputs") {
    createTablesAndThen {
      val p0 = Paths.get("src/test/resources/for-hashing/foo.txt")
      val p1 = Paths.get("src/test/resources/for-hashing/bigger")
      val p2 = Paths.get("src/test/resources/for-hashing/empty.txt")

      assert(dao.allOutputRows.isEmpty)

      val h0 = Hashes.sha1(p0)
      val h1 = Hashes.sha1(p1)
      val h2 = Hashes.sha1(p2)
      
      val rows = Seq(
          cachedOutput(p0, h0),
          cachedOutput(p1, h1),
          cachedOutput(p2, h2))

      dao.insertOrUpdateOutput(rows)
          
      assert(dao.allOutputRows.toSet == rows.toSet)
      
      val h0prime = Hash(Array(0.toByte), HashType.Sha1)
      val h1prime = Hash(Array(1.toByte), HashType.Sha1)
      
      val updatedRows = Seq(
          cachedOutput(p0, h0prime),
          cachedOutput(p1, h1prime))
          
      dao.insertOrUpdateOutput(updatedRows)
      
      val expected = updatedRows :+ cachedOutput(p2, h2)

      assert(dao.allOutputRows.toSet == expected.toSet)
    }
  }
  
  test("insertExecution/allExecutionRows") {
    
  }
}