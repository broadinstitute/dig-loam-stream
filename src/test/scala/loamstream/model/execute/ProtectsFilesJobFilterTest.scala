package loamstream.model.execute

import org.scalatest.FunSuite
import java.net.URI
import loamstream.TestHelpers
import java.nio.file.Path
import java.io.StringReader
import loamstream.util.Files
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.DataHandle

/**
 * @author clint
 * Dec 15, 2020
 */
final class ProtectsFilesJobFilterTest extends FunSuite {
  import URI.{create => uri}
  import TestHelpers.path
  
  test("apply - varags") {
    val filter = ProtectsFilesJobFilter("x", "y", "a", "x")
    
    assert(filter.locationsToProtect === Set("x", "y", "a"))
  }
  
  test("apply - Iterable") {
    val filter = ProtectsFilesJobFilter(Iterable("x", "y", "a", "x"))
    
    assert(filter.locationsToProtect === Set("x", "y", "a"))
  }
  
  test("apply - Iterable of Eithers") {
    val es: Iterable[Either[Path, URI]] = {
      Iterable(Left(path("x")), Right(uri("gs://y")), Left(path("a")), Left(path("x")))
    }
    
    val filter = ProtectsFilesJobFilter(es)
    
    val expected = Set(path("x").toAbsolutePath.toString, "gs://y", path("a").toAbsolutePath.toString)
    
    assert(filter.locationsToProtect === expected)
  }
  
  test("fromString") {
    val data = s"""|x
                   |  //lala
                   |   gs://y
                   | a
                   |#zzzzzz
                   |###
                   | x""".stripMargin
                   
    val expected = Set(path("x").toAbsolutePath.toString, "gs://y", path("a").toAbsolutePath.toString)
    
    val filter = ProtectsFilesJobFilter.fromString(data)
    
    assert(filter.locationsToProtect === expected)
  }
  
  test("fromReader") {
    val data = s"""|x
                   |  //lala
                   |   gs://y
                   | a
                   |#zzzzzz
                   |###
                   | x""".stripMargin
                   
    val expected = Set(path("x").toAbsolutePath.toString, "gs://y", path("a").toAbsolutePath.toString)
    
    val filter = ProtectsFilesJobFilter.fromReader(new StringReader(data))
    
    assert(filter.locationsToProtect === expected)
  }
  
  test("fromFile") {
    val data = s"""|x
                   |  //lala
                   |   gs://y
                   | a
                   |#zzzzzz
                   |###
                   | x""".stripMargin
                
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("asdf")

      Files.writeTo(file)(data)
                   
      val expected = Set(path("x").toAbsolutePath.toString, "gs://y", path("a").toAbsolutePath.toString)
    
      val filter = ProtectsFilesJobFilter.fromFile(file)
    
      assert(filter.locationsToProtect === expected)
    }
  }
  
  test("empty") {
    assert(ProtectsFilesJobFilter.empty.locationsToProtect.isEmpty === true)
  }
  
  test("shouldRun - empty protected set") {
    val filter = ProtectsFilesJobFilter.empty
    
    val job = MockJob(JobStatus.Succeeded, outputs = Set(DataHandle.PathHandle(path("foo"))))
    
    assert(filter.shouldRun(job) === true)
  }
  
  test("shouldRun - some protected files, none apply") {
    fail("TODO")
  }
  
  test("shouldRun - some protected files, some apply") {
    fail("TODO")
  }
}
