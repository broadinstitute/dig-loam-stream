package loamstream.util

import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.model.jobs.MockJob

/**
 * @author clint
 * Nov 15, 2017
 */
final class LogFileNamesTest extends FunSuite {

  import loamstream.TestHelpers.path
  import loamstream.model.jobs.JobStatus.Succeeded

  private val outputDir = path("/w/x/y/z/")

  private def stdout(outDir: Path = outputDir) = LogFileNames.stdout(outDir)
  private def stderr(outDir: Path = outputDir) = LogFileNames.stderr(outDir)
  private def exitCode(outDir: Path = outputDir) = LogFileNames.exitCode(outDir)

  test("stdout - job with generated name") {
    val job = MockJob(Succeeded)

    val fileName = stdout()

    assert(fileName === path(s"/w/x/y/z/stdout").toAbsolutePath)
  }

  test("stdout - job with supplied name") {
    val job = MockJob(Succeeded, "foo")

    val fileName = stdout()

    assert(fileName === path(s"/w/x/y/z/stdout").toAbsolutePath)
  }

  test("stderr - job with generated name") {
    val job = MockJob(Succeeded)

    val fileName = stderr()

    assert(fileName === path(s"/w/x/y/z/stderr").toAbsolutePath)
  }

  test("stderr - job with supplied name") {
    val job = MockJob(Succeeded, "foo")

    val fileName = stderr()

    assert(fileName === path(s"/w/x/y/z/stderr").toAbsolutePath)
  }

  test("exitcode - job with generated name") {
    val job = MockJob(Succeeded)

    val fileName = exitCode()

    assert(fileName === path(s"/w/x/y/z/exitcode").toAbsolutePath)
  }

  test("exitcode - job with supplied name") {
    val job = MockJob(Succeeded, "foo")

    val fileName = exitCode()

    assert(fileName === path(s"/w/x/y/z/exitcode").toAbsolutePath)
  }
  
  test("exitcode - job with supplied name, custom dir") {
    val job = MockJob(Succeeded, "foo")

    val fileName = exitCode(path("blah"))

    assert(fileName === path(s"blah/exitcode").toAbsolutePath)
  }
  
  test("stdout - name with 'bad' chars") {
    val job = MockJob(Succeeded, "foo   blah/blah:bar\\baz")

    val fileName = stdout()

    assert(fileName === path(s"/w/x/y/z/stdout").toAbsolutePath)
  }

  test("stderr - name with 'bad' chars") {
    val fileName = stderr()

    assert(fileName === path(s"/w/x/y/z/stderr").toAbsolutePath)
  }

  test("stdout - job with supplied name, custom dir") {
    val job = MockJob(Succeeded, "foo")

    val fileName = stdout(path("blah"))

    assert(fileName === path(s"blah/stdout").toAbsolutePath)
  }

  test("stderr - job with supplied name, custom dir") {
    val job = MockJob(Succeeded, "foo")

    val fileName = stderr(path("blah"))

    assert(fileName === path(s"blah/stderr").toAbsolutePath)
  }
}
