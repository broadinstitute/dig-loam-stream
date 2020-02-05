package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob

/**
 * @author clint
 * Jan 31, 2020
 */
final class ExecutionStateTest extends FunSuite {
  test("initialFor") {
    val j0 = MockJob(JobStatus.Succeeded)
    val j1 = MockJob(JobStatus.Succeeded)
    val j2 = MockJob(JobStatus.Succeeded)
    
    val executable = Executable(Set(j0, j1, j2))
    
    val maxRunsPerJob = 42
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    assert(state.maxRunsPerJob === maxRunsPerJob)
    assert(state.allJobs.toSet === Set(j0, j1, j2))
    
    assert(state.statusOf(j0) === JobStatus.NotStarted)
    assert(state.statusOf(j1) === JobStatus.NotStarted)
    assert(state.statusOf(j2) === JobStatus.NotStarted)
  }
  
  test("isFinished/finish - no restarts") {
    val j0 = MockJob(JobStatus.Succeeded)
    val j1 = MockJob(JobStatus.Succeeded)
    val j2 = MockJob(JobStatus.Succeeded)
    
    val executable = Executable(Set(j0, j1, j2))
    
    val maxRunsPerJob = 1
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    state.startRunning(Seq(j0, j1, j2))
    
    assert(state.isFinished === false)
    
    state.finish(j0, JobStatus.Skipped)
    
    assert(state.isFinished === false)
    
    state.finish(j2, JobStatus.Succeeded)
    
    assert(state.isFinished === false)
    
    state.finish(j1, JobStatus.Failed)
    
    assert(state.isFinished === true)
    
    assert(state.statusOf(j0) === JobStatus.Skipped)
    assert(state.statusOf(j2) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
  }
  
  test("isFinished/finish - some restarts") {
    val j0 = MockJob(JobStatus.Succeeded)
    val j1 = MockJob(JobStatus.Succeeded)
    val j2 = MockJob(JobStatus.Succeeded)
    
    val executable = Executable(Set(j0, j1, j2))
    
    val maxRunsPerJob = 2
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    state.startRunning(Seq(j0, j1, j2))
    
    assert(state.statusOf(j0) === JobStatus.Running)
    assert(state.statusOf(j2) === JobStatus.Running)
    assert(state.statusOf(j1) === JobStatus.Running)
    
    assert(state.isFinished === false)
    
    state.finish(j0, JobStatus.Skipped)
    
    assert(state.isFinished === false)
    
    state.finish(j2, JobStatus.Succeeded)
    
    assert(state.isFinished === false)
    
    state.finish(j1, JobStatus.Failed)
    
    assert(state.isFinished === false)
    
    assert(state.statusOf(j0) === JobStatus.Skipped)
    assert(state.statusOf(j2) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.NotStarted)
    
    state.startRunning(Seq(j1))
    
    state.finish(j1, JobStatus.Failed)
    
    assert(state.isFinished === true)
    
    assert(state.statusOf(j0) === JobStatus.Skipped)
    assert(state.statusOf(j2) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
  }
  
  test("startRunning / reRun") {
    val j0 = MockJob(JobStatus.Succeeded)
    val j1 = MockJob(JobStatus.Succeeded)
    val j2 = MockJob(JobStatus.Succeeded)
    
    val executable = Executable(Set(j0, j1, j2))
    
    val maxRunsPerJob = 42
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    assert(state.statusOf(j0) === JobStatus.NotStarted)
    assert(state.statusOf(j1) === JobStatus.NotStarted)
    assert(state.statusOf(j2) === JobStatus.NotStarted)
    
    state.startRunning(Nil)
    
    assert(state.statusOf(j0) === JobStatus.NotStarted)
    assert(state.statusOf(j1) === JobStatus.NotStarted)
    assert(state.statusOf(j2) === JobStatus.NotStarted)
    
    state.startRunning(Seq(j1, j2))
    
    assert(state.statusOf(j0) === JobStatus.NotStarted)
    assert(state.statusOf(j1) === JobStatus.Running)
    assert(state.statusOf(j2) === JobStatus.Running)
    
    state.reRun(Seq(j1, j2))
    
    assert(state.statusOf(j0) === JobStatus.NotStarted)
    assert(state.statusOf(j1) === JobStatus.NotStarted)
    assert(state.statusOf(j2) === JobStatus.NotStarted) 
  }
  
  test("eligibleToRun - all jobs eligible") {
    val j0 = MockJob(JobStatus.Succeeded)
    val j1 = MockJob(JobStatus.Succeeded)
    val j2 = MockJob(JobStatus.Succeeded)
    
    val executable = Executable(Set(j0, j1, j2))
    
    val maxRunsPerJob = 42
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    assert(state.eligibleToRun.toSet === Set(j0, j1, j2))
  }
  
  test("eligibleToRun - some jobs eligible") {
    /*
     *   j0 -- j1
     *           \
     *            + -- j3
     *           /
     *         j2
     */
    val j0 = MockJob(JobStatus.Succeeded)
    val j1 = MockJob(JobStatus.Succeeded, dependencies = Set(j0))
    val j2 = MockJob(JobStatus.Succeeded)
    val j3 = MockJob(JobStatus.Succeeded, dependencies = Set(j2, j1))
    
    val executable = Executable(Set(j3))
    
    val maxRunsPerJob = 42
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    assert(state.eligibleToRun.toSet === Set(j2, j0))
  }
  
  test("eligibleToRun - no jobs eligible") {
    /*
     *   j0 -- j1
     *           \
     *            + -- j3
     *           /
     *         j2
     */
    val j0 = MockJob(JobStatus.Succeeded)
    val j1 = MockJob(JobStatus.Succeeded, dependencies = Set(j0))
    val j2 = MockJob(JobStatus.Succeeded)
    val j3 = MockJob(JobStatus.Succeeded, dependencies = Set(j2, j1))
    
    val executable = Executable(Set(j3))
    
    val maxRunsPerJob = 42
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    Seq(j0, j1, j2, j3).foreach(job => state.finish(job, JobStatus.Succeeded))
        
    assert(state.isFinished === true)
        
    assert(state.eligibleToRun.toSet === Set.empty)
  }
  
  test("jobStatuses") {
    /*
     *    j0 (s)
     *       \  
     *        j2 (cns)
     *       /   \
     *    j1 (f)  j4
     *           /
     *         j3 (s)
     */
    val j0 = MockJob(JobStatus.Succeeded, name = "j0")
    val j1 = MockJob(JobStatus.Failed, name = "j1")
    val j2 = MockJob(JobStatus.CouldNotStart, name = "j2", dependencies = Set(j0, j1))
    val j3 = MockJob(JobStatus.Succeeded, name = "j3")
    val j4 = MockJob(JobStatus.Succeeded, name = "j4", dependencies = Set(j2, j3))
    
    val executable = Executable(Set(j4))
    
    val maxRunsPerJob = 1
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    val jobStatuses0 = state.jobStatuses
    
    state.allJobs.foreach { job => assert(state.statusOf(job) === JobStatus.NotStarted) }
    
    assert(jobStatuses0.readyToRun.keySet === Set(j0, j1, j3))
    assert(jobStatuses0.cannotRun.keySet === Set.empty)
    
    state.startRunning(Seq(j0))
    state.finish(j0, JobStatus.Succeeded)
    
    assert(state.statusOf(j0) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.NotStarted)
    assert(state.statusOf(j2) === JobStatus.NotStarted)
    assert(state.statusOf(j3) === JobStatus.NotStarted)
    assert(state.statusOf(j4) === JobStatus.NotStarted)
    
    val jobStatuses1 = state.jobStatuses
    
    assert(jobStatuses1.readyToRun.keySet === Set(j1, j3))
    assert(jobStatuses1.cannotRun.keySet === Set.empty)
    
    state.startRunning(Seq(j1))
    state.finish(j1, JobStatus.FailedPermanently)
    
    assert(state.statusOf(j0) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
    assert(state.statusOf(j2) === JobStatus.NotStarted)
    assert(state.statusOf(j3) === JobStatus.NotStarted)
    assert(state.statusOf(j4) === JobStatus.NotStarted)
    
    val jobStatuses2 = state.jobStatuses
    
    assert(jobStatuses2.readyToRun.keySet === Set(j3))
    assert(jobStatuses2.cannotRun.keySet === Set(j2))
    
    state.startRunning(Seq(j3))
    state.finish(j3, JobStatus.Succeeded)
    
    assert(state.statusOf(j0) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
    assert(state.statusOf(j2) === JobStatus.NotStarted)
    assert(state.statusOf(j3) === JobStatus.Succeeded)
    assert(state.statusOf(j4) === JobStatus.NotStarted)
    
    val jobStatuses3 = state.jobStatuses
    
    assert(jobStatuses3.readyToRun.keySet === Set.empty)
    assert(jobStatuses3.cannotRun.keySet.map(_.name) === Set(j2.name))
    
    state.startRunning(Seq(j2))
    state.finish(j2, JobStatus.CouldNotStart)
    
    assert(state.statusOf(j0) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
    assert(state.statusOf(j2) === JobStatus.CouldNotStart)
    assert(state.statusOf(j3) === JobStatus.Succeeded)
    assert(state.statusOf(j4) === JobStatus.NotStarted)
    
    val jobStatuses4 = state.jobStatuses
    
    assert(jobStatuses4.readyToRun.keySet === Set.empty)
    assert(jobStatuses4.cannotRun.keySet.map(_.name) === Set(j4.name))
    
    state.startRunning(Seq(j4))
    state.finish(j4, JobStatus.CouldNotStart)
    
    val jobStatuses5 = state.jobStatuses
    
    assert(jobStatuses5.readyToRun.keySet === Set.empty)
    assert(jobStatuses5.cannotRun.keySet === Set.empty)
    
    assert(state.isFinished)
  }
  
  test("updateJobs") {
    /*
     *    j0 (s)
     *       \  
     *        j2 (cns)
     *       /   \
     *    j1 (f)  j4
     *           /
     *         j3 (s)
     */
    val j0 = MockJob(JobStatus.Succeeded, name = "j0")
    val j1 = MockJob(JobStatus.Failed, name = "j1")
    val j2 = MockJob(JobStatus.CouldNotStart, name = "j2", dependencies = Set(j0, j1))
    val j3 = MockJob(JobStatus.Succeeded, name = "j3")
    val j4 = MockJob(JobStatus.Succeeded, name = "j4", dependencies = Set(j2, j3))
    
    val executable = Executable(Set(j4))
    
    val maxRunsPerJob = 1
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    state.allJobs.foreach { job => assert(state.statusOf(job) === JobStatus.NotStarted) }
    
    val jobStatuses0 = state.updateJobs()
    
    assert(state.statusOf(j0) === JobStatus.Running)
    assert(state.statusOf(j1) === JobStatus.Running)
    assert(state.statusOf(j2) === JobStatus.NotStarted)
    assert(state.statusOf(j3) === JobStatus.Running)
    assert(state.statusOf(j4) === JobStatus.NotStarted)
    
    state.finish(j0, JobStatus.Succeeded)
    state.finish(j1, JobStatus.FailedPermanently)
    state.finish(j3, JobStatus.Succeeded)
    
    assert(jobStatuses0.readyToRun.keySet === Set(j0, j1, j3))
    assert(jobStatuses0.cannotRun.keySet === Set.empty)
    
    val jobStatuses1 = state.updateJobs()
    
    assert(state.statusOf(j0) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
    assert(state.statusOf(j2) === JobStatus.CouldNotStart)
    assert(state.statusOf(j3) === JobStatus.Succeeded)
    assert(state.statusOf(j4) === JobStatus.NotStarted)
    
    assert(jobStatuses1.readyToRun.keySet === Set.empty)
    assert(jobStatuses1.cannotRun.keySet === Set(j2))
    
    val jobStatuses2 = state.updateJobs()
    
    assert(state.statusOf(j0) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
    assert(state.statusOf(j2) === JobStatus.CouldNotStart)
    assert(state.statusOf(j3) === JobStatus.Succeeded)
    assert(state.statusOf(j4) === JobStatus.CouldNotStart)
    
    assert(jobStatuses2.readyToRun.keySet === Set.empty)
    assert(jobStatuses2.cannotRun.keySet === Set(j4))
    
    assert(state.isFinished)
    
    val jobStatuses3 = state.updateJobs()
    
    assert(state.statusOf(j0) === JobStatus.Succeeded)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
    assert(state.statusOf(j2) === JobStatus.CouldNotStart)
    assert(state.statusOf(j3) === JobStatus.Succeeded)
    assert(state.statusOf(j4) === JobStatus.CouldNotStart)
    
    assert(jobStatuses3.readyToRun.keySet === Set.empty)
    assert(jobStatuses3.cannotRun.keySet === Set.empty)
  }

  test("cancelSuccessors") {
    /*
     *    j0 (s)
     *       \  
     *        j2 (cns)
     *       /   \
     *    j1 (f)  j4
     *           /
     *         j3 (s)
     */
    lazy val j0: MockJob = MockJob(JobStatus.Succeeded, name = "j0", successorsFn = () => Set(j2))
    lazy val j1: MockJob = MockJob(JobStatus.Failed, name = "j1", successorsFn = () => Set(j2))
    lazy val j2: MockJob = MockJob(JobStatus.CouldNotStart, name = "j2", dependencies = Set(j0, j1), successorsFn = () => Set(j4))
    lazy val j3: MockJob = MockJob(JobStatus.Succeeded, name = "j3", successorsFn = () => Set(j4))
    lazy val j4: MockJob = MockJob(JobStatus.CouldNotStart, name = "j4", dependencies = Set(j2, j3))
    
    val executable = Executable(Set(j4))
    
    val maxRunsPerJob = 1
    
    val state = ExecutionState.initialFor(executable, maxRunsPerJob)
    
    state.allJobs.foreach { job => assert(state.statusOf(job) === JobStatus.NotStarted) }
    
    state.startRunning(Seq(j0, j1))
    
    assert(state.statusOf(j0) === JobStatus.Running)
    assert(state.statusOf(j1) === JobStatus.Running)
    assert(state.statusOf(j2) === JobStatus.NotStarted)
    assert(state.statusOf(j3) === JobStatus.NotStarted)
    assert(state.statusOf(j4) === JobStatus.NotStarted)
    
    state.finish(j1, JobStatus.Failed)
    
    assert(state.statusOf(j0) === JobStatus.Running)
    assert(state.statusOf(j1) === JobStatus.FailedPermanently)
    assert(state.statusOf(j2) === JobStatus.CouldNotStart)
    assert(state.statusOf(j3) === JobStatus.NotStarted)
    assert(state.statusOf(j4) === JobStatus.CouldNotStart)
  }
}
