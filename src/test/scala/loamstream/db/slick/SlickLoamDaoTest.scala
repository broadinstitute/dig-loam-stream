package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.model.jobs.Output.CachedOutput
import loamstream.util.Hash
import loamstream.util.HashType
import loamstream.util.Hashes
import loamstream.model.jobs.Execution


/**
 * @author clint
 * date: Aug 9, 2016
 */
final class SlickLoamDaoTest extends FunSuite with ProvidesSlickLoamDao {
  
  override val descriptor = TestDbDescriptors.inMemoryH2
  
  private val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
  private val path1 = Paths.get("src/test/resources/for-hashing/bigger")
  private val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")
  
  private lazy val hash0 = Hashes.sha1(path0)
  private lazy val hash1 = Hashes.sha1(path1)
  private lazy val hash2 = Hashes.sha1(path2)
  
  private def noOutputs: Boolean = dao.allOutputs.isEmpty
  
  private def noExecutions: Boolean = dao.allExecutions.isEmpty
  
  private def store(paths: Path*): Unit = {
    val outputs = paths.map { path =>
      val hash = Hashes.sha1(path)
      
      cachedOutput(path, hash)
    }
    
    val execution = Execution(1, outputs.toSet)
    
    dao.insertExecutions(execution)
  }
  
  test("insert/Read Outputs") {
    createTablesAndThen {
      assert(noOutputs)
      
      store(path0)
      
      assert(dao.allOutputs === Seq(cachedOutput(path0, hash0)))
    }
  }

  test("insert/allOutputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      val expected = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1), cachedOutput(path2, hash2))

      //Use Sets to ignore order
      assert(dao.allOutputs.toSet == expected)
    }
  }

  test("delete all Outputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allOutputs.size == 3)

      dao.deleteOutput(path0, path1, path2)

      assert(noOutputs)
    }
  }
  
  test("delete some Outputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allOutputs.size == 3)

      dao.deleteOutput(path0, path2)

      assert(dao.allOutputs.map(_.path) == Seq(path1.toAbsolutePath))
      
      dao.deleteOutput(path0, path1)
      
      assert(noOutputs)
    }
  }
  
  /*test("insertOrUpdateOutputs") {
    createTablesAndThen {

      assert(noOutputs)

      val rows = Seq(
          cachedOutput(path0, hash0),
          cachedOutput(path1, hash1),
          cachedOutput(path2, hash2))

      dao.insertOrUpdateOutputs(rows)
          
      assert(dao.allOutputs.toSet == rows.toSet)
      
      val h0prime = Hash(Array(0.toByte), HashType.Sha1)
      val h1prime = Hash(Array(1.toByte), HashType.Sha1)
      
      val updatedRows = Seq(
          cachedOutput(path0, h0prime),
          cachedOutput(path1, h1prime))
          
      dao.insertOrUpdateOutputs(updatedRows)
      
      val expected = updatedRows :+ cachedOutput(path2, hash2)

      assert(dao.allOutputs.toSet == expected.toSet)
    }
  }*/
  
  test("insertExecutions/allExecutionRows") {
    createTablesAndThen {
      val status0 = 42
      val status1 = 0
      val status2 = 1
      
      val output0 = cachedOutput(path0, hash0)
      val output1 = cachedOutput(path1, hash1)
      val output2 = cachedOutput(path2, hash2)
      
      val ex0 = Execution(status0, Set(output0))
      val ex1 = Execution(status1, Set(output1, output2))
      val ex2 = Execution(status2, Set.empty)
      
      assert(noOutputs)
      assert(noExecutions)

      dao.insertExecutions(ex0)
      
      assert(dao.allExecutions.toSet === Set(ex0))
      
      dao.insertExecutions(ex1, ex2)
      
      assert(dao.allExecutions.toSet === Set(ex0, ex1, ex2))
    }
  }
}