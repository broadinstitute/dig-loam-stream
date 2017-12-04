package loamstream.model.jobs

import org.scalatest.FunSuite
import loamstream.TestHelpers
import java.nio.file.Path

/**
 * @author clint
 * Nov 15, 2017
 */
final class LogFileNamesTest extends FunSuite {
  
  import JobStatus.Succeeded
  import TestHelpers.path
  
  private val outputDir = path("/x/y/z/job-outputs")
  
  private def stdout(job: LJob, outDir: Path = outputDir) = LogFileNames.stdout(job, outDir)
  private def stderr(job: LJob, outDir: Path = outputDir) = LogFileNames.stderr(job, outDir)
  
  test("stdout - job with generated name") {
    val job = MockJob(Succeeded)
    
    val fileName = stdout(job)
    
    assert(fileName === path(s"/x/y/z/job-outputs/${job.name}.stdout"))
  }
  
  test("stdout - job with supplied name") {
    val job = MockJob(Succeeded, "foo")
    
    val fileName = stdout(job)
    
    assert(fileName === path(s"/x/y/z/job-outputs/foo.stdout"))
  }
  
  test("stderr - job with generated name") {
    val job = MockJob(Succeeded)
    
    val fileName = stderr(job)
    
    assert(fileName === path(s"/x/y/z/job-outputs/${job.name}.stderr"))
  }
  
  test("stderr - job with supplied name") {
    val job = MockJob(Succeeded, "foo")
    
    val fileName = stderr(job)
    
    assert(fileName === path(s"/x/y/z/job-outputs/foo.stderr"))
  }
  
  test("stdout - name with 'bad' chars") {
    val job = MockJob(Succeeded, "foo   blah/blah:bar\\baz")
    
    val fileName = stdout(job)
    
    assert(fileName === path(s"/x/y/z/job-outputs/foo___blah_blah_bar_baz.stdout"))
  }
  
  test("stderr - name with 'bad' chars") {
    val job = MockJob(Succeeded, "foo   blah/blah:bar\\baz")
    
    val fileName = stderr(job)
    
    assert(fileName === path(s"/x/y/z/job-outputs/foo___blah_blah_bar_baz.stderr"))
  }
  
  test("stdout - job with supplied name, custom dir") {
    val job = MockJob(Succeeded, "foo")
    
    val fileName = stdout(job, path("blah"))
    
    assert(fileName === path(s"blah/foo.stdout").toAbsolutePath)
  }
  
  test("stderr - job with supplied name, custom dir") {
    val job = MockJob(Succeeded, "foo")
    
    val fileName = stderr(job, path("blah"))
    
    assert(fileName === path(s"blah/foo.stderr").toAbsolutePath)
  }
}