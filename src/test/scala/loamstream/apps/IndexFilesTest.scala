package loamstream.apps

import java.nio.file.Files.exists
import java.time.Instant

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.TestHelpers.path
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.MockJob
import loamstream.util.Files
import loamstream.util.MissingFileTimeoutException


/**
 * @author clint
 * Jun 4, 2019
 */
final class IndexFilesTest extends FunSuite {
    private val j0 = MockJob(JobStatus.Succeeded)
    private val j1 = MockJob(JobStatus.CouldNotStart)
    private val j2 = MockJob(JobStatus.Failed)
    
    private val r0: Option[JobResult] = Some(CommandResult(0))
    private val r1: Option[JobResult] = None
    private val r2: Option[JobResult] = Some(CommandResult(42))
    
    private val t0 = Instant.now
    private val t1 = Instant.ofEpochMilli(t0.toEpochMilli + 1)
    private val t2 = Instant.ofEpochMilli(t0.toEpochMilli + 2)
    
    private val p0 = path("jobs/job0")
    private val p1 = path("jobs/job1")
    private val p2 = path("jobs/job2")
    
    private val e0 = Execution.from(
        job = j0, 
        status = j0.toReturn.jobStatus, 
        result = r0, 
        resources = Some(LocalResources(t0, t0)), 
        jobDir = Some(p0))
        
    private val e1 = Execution.from(
        job = j1, 
        status = j1.toReturn.jobStatus, 
        result = r1,
        resources = Some(LocalResources(t1, t1)), 
        jobDir = None)
        
    private val e2 = Execution.from(
        job = j2, 
        status = j2.toReturn.jobStatus, 
        result = r2,
        resources = Some(LocalResources(t2, t2)), 
        jobDir = Some(p2))
        
  test("writeIndexFiles") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val logDir = workDir.resolve("logs")
      
      val executionConfig = ExecutionConfig.default.copy(logDir = logDir)
      
      val allJobsFile = logDir.resolve("all-jobs.tsv")
      val failedJobsFile = logDir.resolve("failed-jobs.tsv")
      
      assert(exists(allJobsFile) === false)
      assert(exists(failedJobsFile) === false)
      
      val jobsToExecutions: Map[LJob, Execution] = Map(j0 -> e0, j1 -> e1, j2 -> e2)
      
      IndexFiles.writeIndexFiles(executionConfig, jobsToExecutions)
      
      assert(exists(allJobsFile) === true)
      assert(exists(failedJobsFile) === true)
      
      val allJobsContent = Files.readFrom(allJobsFile)
      val failedJobsContent = Files.readFrom(failedJobsFile)
      
      val tab = '\t'
      val newline = scala.util.Properties.lineSeparator
      
      val expectedAllJobsContent = s"""
JOB_ID${tab}JOB_NAME${tab}JOB_STATUS${tab}EXIT_CODE${tab}JOB_DIR
${j0.id}${tab}${j0.name}${tab}Succeeded${tab}0${tab}${p0.toAbsolutePath}
${j1.id}${tab}${j1.name}${tab}CouldNotStart${tab}<not available>${tab}<not available>
${j2.id}${tab}${j2.name}${tab}Failed${tab}42${tab}${p2.toAbsolutePath}""".trim ++ newline

      val expectedFailedJobsContent = s"""
JOB_ID${tab}JOB_NAME${tab}JOB_STATUS${tab}EXIT_CODE${tab}JOB_DIR
${j2.id}${tab}${j2.name}${tab}Failed${tab}42${tab}${p2.toAbsolutePath}""".trim ++ newline

      assert(allJobsContent === expectedAllJobsContent)

      assert(failedJobsContent === expectedFailedJobsContent)
    }
  }
    
  test("writeIndexFiles - one exception") {
    def doTest(makeResult: Exception => JobResult): Unit = {
      TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
        val logDir = workDir.resolve("logs")
        
        val executionConfig = ExecutionConfig.default.copy(logDir = logDir)
        
        val allJobsFile = logDir.resolve("all-jobs.tsv")
        val failedJobsFile = logDir.resolve("failed-jobs.tsv")
        
        assert(exists(allJobsFile) === false)
        assert(exists(failedJobsFile) === false)
        
        val exception = new Exception("foo") with scala.util.control.NoStackTrace
        
        val e1Prime = e1.copy(
            status = JobStatus.FailedWithException, 
            result = Some(makeResult(exception)),
            jobDir = Some(p1))
        
        val jobsToExecutions: Map[LJob, Execution] = Map(j1 -> e1Prime)
        
        IndexFiles.writeIndexFiles(executionConfig, jobsToExecutions)
        
        assert(exists(allJobsFile) === true)
        assert(exists(failedJobsFile) === true)
        
        val allJobsContent = Files.readFrom(allJobsFile)
        val failedJobsContent = Files.readFrom(failedJobsFile)
        
        val tab = '\t'
        val newline = scala.util.Properties.lineSeparator
        
        val expectedFailedJobsContent = s"""
JOB_ID${tab}JOB_NAME${tab}JOB_STATUS${tab}EXIT_CODE${tab}JOB_DIR
${j1.id}${tab}${j1.name}${tab}Failed due to exception: 'foo'${tab}<not available>${tab}${p1.toAbsolutePath}""".trim ++ 
newline
  
        val expectedAllJobsContent = expectedFailedJobsContent
  
        assert(allJobsContent === expectedAllJobsContent)
  
        assert(failedJobsContent === expectedFailedJobsContent)
      }
    }
    
    doTest(JobResult.CommandInvocationFailure(_))
    doTest(JobResult.FailureWithException(_))
  }
}
