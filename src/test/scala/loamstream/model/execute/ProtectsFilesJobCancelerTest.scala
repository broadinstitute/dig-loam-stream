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
final class ProtectsFilesJobCancelerTest extends FunSuite {
  import URI.{create => uri}
  import TestHelpers.path
  
  test("apply - varags") {
    val filter = ProtectsFilesJobCanceler("x", "y", "a", "x")
    
    assert(filter.locationsToProtect === Set("x", "y", "a"))
  }
  
  test("apply - Iterable") {
    val filter = ProtectsFilesJobCanceler(Iterable("x", "y", "a", "x"))
    
    assert(filter.locationsToProtect === Set("x", "y", "a"))
  }
  
  test("fromEithers - Iterable of Eithers") {
    val es: Iterable[Either[Path, URI]] = {
      Iterable(Left(path("x")), Right(uri("gs://y")), Left(path("a")), Left(path("x")))
    }
    
    val filter = ProtectsFilesJobCanceler.fromEithers(es)
    
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
    
    val filter = ProtectsFilesJobCanceler.fromString(data)
    
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
    
    val filter = ProtectsFilesJobCanceler.fromReader(new StringReader(data))
    
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
    
      val filter = ProtectsFilesJobCanceler.fromFile(file)
    
      assert(filter.locationsToProtect === expected)
    }
  }
  
  test("empty") {
    assert(ProtectsFilesJobCanceler.empty.locationsToProtect.isEmpty === true)
  }
  
  test("shouldCancel - empty protected set") {
    val filter = ProtectsFilesJobCanceler.empty
    
    val loneOutput = DataHandle.PathHandle(path("foo"))
    
    assert(loneOutput.isMissing)
    
    //The one output being missing means the job should run; despite this, it's not eligible for cancellation
    //since there are no protected outputs.
    
    val job = MockJob(JobStatus.Succeeded, outputs = Set(loneOutput))
    
    assert(filter.shouldCancel(job) === false)
  }
  
  test("shouldCancel - some protected files, none apply") {
    val filter = ProtectsFilesJobCanceler(Iterable("x", "y", "a", "x"))
    
    val loneOutput = DataHandle.PathHandle(path("z"))
    
    assert(loneOutput.isMissing)
    
    //The one output being missing means the job should run; despite this, it's not eligible for cancellation
    //since none of the protected outputs applies.
    
    val job = MockJob(JobStatus.Succeeded, outputs = Set(DataHandle.PathHandle(path("z"))))
    
    assert(filter.shouldCancel(job) === false)
  }
  
  test("shouldCancel - some protected files, some apply") {
    val locs = Iterable(Left(path("x")), Right(uri("gs://y")), Left(path("a")), Left(path("x")))
    
    val filter = ProtectsFilesJobCanceler.fromEithers(locs)
    
    def pathHandle(s: String) = DataHandle.PathHandle(path(s))
    
    val outputs: Set[DataHandle] = Set(pathHandle("y"), pathHandle("a"))
    
    assert(outputs.exists(_.isMissing))
    
    //At least one output being missing means the job should run; it's eligible for cancellation
    //since some of the protected outputs apply.
    
    val job = MockJob(JobStatus.Succeeded, outputs = outputs)
    
    assert(filter.shouldCancel(job) === true)
  }
}
