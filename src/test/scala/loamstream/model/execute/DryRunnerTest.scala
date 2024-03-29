package loamstream.apps

import org.scalatest.FunSuite

import loamstream.model.execute.ByNameJobFilter
import loamstream.model.execute.DryRunner
import loamstream.model.execute.Executable
import loamstream.model.execute.JobFilter
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.MockJob

import scala.collection.compat._
import loamstream.model.jobs.JobNode

/**
 * @author clint
 * Feb 15, 2018
 */
final class DryRunnerTest extends FunSuite {

  import DryRunnerTest.MockJobFilter
  
  private def sortedById(jobs: LJob*): Seq[LJob] = jobs.sortBy(_.id)
  
  //NB: We just need any job; the status the job would produce if it was ran is irrelevant, so just say 'Succeeded'. 
  private def mockJob = MockJob(JobStatus.Succeeded)
  
  private def mockJob(name: String) = MockJob(toReturn = JobStatus.Succeeded, name = name)
  
  private def mockJob(name: String, dependencies: MockJob*): MockJob = {
    MockJob(toReturn = JobStatus.Succeeded, name = name, dependencies = (dependencies.toSeq: Seq[JobNode]).to(Set))
  }
  
  test("toBeRun - single job, should be run") {
    //j0 
    
    val job = mockJob
    
    val executable = Executable(Set(job))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    assert(actual === Seq(job))
  }
  
  test("toBeRun - single job, should NOT be run") {
    //j0 (x) 
    
    val job = mockJob
    
    val executable = Executable(Set(job))
    
    val jobFilter = MockJobFilter(shouldNOTRun = Set(job))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    assert(actual === Nil)
  }
  
  test("toBeRun - two jobs, should be run") {
    /*
     * j0 
     * 
     * j1 
     */
    
    val job0 = mockJob
    val job1 = mockJob
    
    val executable = Executable(Set(job0, job1))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0, job1))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    assert(actual === sortedById(job0, job1))
  }
  
  test("toBeRun - two jobs, should NOT be run") {
    /*
     * j0 (x) 
     * 
     * j1 (x) 
     */
    
    val job0 = mockJob
    val job1 = mockJob
    
    val executable = Executable(Set(job0, job1))
    
    val jobFilter = MockJobFilter(shouldNOTRun = Set(job0, job1))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    assert(actual === Nil)
  }
  
  test("toBeRun - two jobs, only one should be run") {
    /*
     * j0 (x)
     * 
     * j1
     */
    
    val job0 = mockJob
    val job1 = mockJob
    
    val executable = Executable(Set(job0, job1))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job1), shouldNOTRun = Set(job0))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    assert(actual === Seq(job1))
  }
  
  test("toBeRun - job with one dep, everything should be run") {
    //j0 => j1
    
    val job0 = mockJob("j0")
    val job1 = mockJob("j1", job0)
    
    val executable = Executable(Set(job1))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0, job1))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    assert(actual === Seq(job0, job1))
  }
  
  test("toBeRun - longer linear pipeline, everything should be run") {
    //j0 => j1 => j2 => j3
    
    val job0 = mockJob("j0")
    val job1 = mockJob("j1", job0)
    val job2 = mockJob("j2", job1)
    val job3 = mockJob("j3", job2)
    
    val executable = Executable(Set(job3))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0, job1, job2, job3))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    assert(actual === Seq(job0, job1, job2, job3))
  }
  
  test("toBeRun - longer linear pipeline, first job shouldn't run") {
    //j0 (x) => j1 => j2 => j3
    
    val job0 = mockJob("j0")
    val job1 = mockJob("j1", job0)
    val job2 = mockJob("j2", job1)
    val job3 = mockJob("j3", job2)
    
    val executable = Executable(Set(job3))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job1, job2, job3), shouldNOTRun = Set(job0))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    assert(actual === Seq(job1, job2, job3))
  }
  
  test("toBeRun - longer linear pipeline, only first job should run") {
    //j0 => j1 (x) => j2 (x) => j3 (x)
    val job0 = mockJob("j0")
    val job1 = mockJob("j1", job0)
    val job2 = mockJob("j2", job1)
    val job3 = mockJob("j3", job2)
    
    val executable = Executable(Set(job3))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0), shouldNOTRun = Set(job1, job2, job3))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    //NB: DryRunner is 'conservative'; if the JobFilter says a job should run, but subsequent jobs shouldn't, 
    //indicate that the subsequnt jobs will run since they follow on from one that does.
    assert(actual === Seq(job0, job1, job2, job3))
  }
  
  test("toBeRun - diamond-shaped pipeline, everything should run") {
    /*    
     *    j1
     *   /  \
     * j0    j3
     *   \  /
     *    j2
     */
    
    val job0 = mockJob("j0")
    val job1 = mockJob("j1", job0)
    val job2 = mockJob("j2", job0)
    val job3 = mockJob("j3", job1, job2)
    
    val executable = Executable(Set(job3))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0, job1, job2, job3))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    //NB: DryRunner is 'conservative'; if the JobFilter says a job should run, but subsequent jobs shouldn't, 
    //indicate that the subsequnt jobs will run since they follow on from one that does.
    assert(actual === Seq(job0, job1, job2, job3))
  }
  
  test("toBeRun - diamond-shaped pipeline, only first job should run") {
    /*    
     *    j1 (x)
     *   /  \
     * j0    j3 (x)
     *   \  /
     *    j2 (x)
     */
    
    val job0 = mockJob("j0")
    val job1 = mockJob("j1", job0)
    val job2 = mockJob("j2", job0)
    val job3 = mockJob("j3", job1, job2)
    
    val executable = Executable(Set(job3))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0), shouldNOTRun = Set(job1, job2, job3))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    //NB: DryRunner is 'conservative'; if the JobFilter says a job should run, but subsequent jobs shouldn't, 
    //indicate that the subsequnt jobs will run since they follow on from one that does.
    assert(actual === Seq(job0, job1, job2, job3))
  }
  
  test("toBeRun - diamond-shaped pipeline, wide fanout, only first job should run") {
    /*    
     *     j1 (x)
     *    /   \
     * j0  ...  j100 (x)
     *    \   /
     *     j99 (x)
     */
    
    val job0 = mockJob("j0")
    val jobs1to99 = (1 to 99).map(i => mockJob(s"j$i", job0))
    val job100 = mockJob("j100", jobs1to99: _*)
    
    assert(jobs1to99.sortBy(_.id.toInt) === jobs1to99)
    
    val executable = Executable(Set(job100))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0), shouldNOTRun = ((jobs1to99: Seq[LJob]).to(Set) + job100))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    //NB: DryRunner is 'conservative'; if the JobFilter says a job should run, but subsequent jobs shouldn't, 
    //indicate that the subsequnt jobs will run since they follow on from one that does.
    assert(actual === (job0 +: jobs1to99 :+ job100))
  }
  
  test("toBeRun - more-complex topology, wide fanout only first job should run") {
    /*    
     *     j1 (x)
     *    /   \
     * j0  ... +------ j100 (x)
     *   |\   /          |
     *   | j99 (x)       |
     *   |               |
     *   +---------------+
     */
    
    val job0 = mockJob("j0")
    val jobs1to99 = (1 to 99).map(i => mockJob(s"j$i", job0))
    val job100 = mockJob("j100", (job0 +: jobs1to99): _*)
    
    assert(jobs1to99.sortBy(_.id.toInt) === jobs1to99)
    
    val executable = Executable(Set(job100))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0), shouldNOTRun = ((jobs1to99: Seq[LJob]).to(Set) + job100))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    //NB: DryRunner is 'conservative'; if the JobFilter says a job should run, but subsequent jobs shouldn't, 
    //indicate that the subsequnt jobs will run since they follow on from one that does.
    assert(actual === (job0 +: jobs1to99 :+ job100))
  }
  
  test("toBeRun - more-complex topology, some jobs should run") {
    /*    
     *     j1 (x)
     *    /   \
     * j0  ... +--j3---- j5 (x)
     *   |\   /          |
     *   | j2 (x)        |
     *   |               |
     *   +--------j4-----+
     */
    
    val job0 = mockJob("j0")
    val job1 = mockJob("j1", job0)
    val job2 = mockJob("j2", job0)
    val job3 = mockJob("j3", job1, job2)
    val job4 = mockJob("j4", job0)
    val job5 = mockJob("j5", job3, job4)
    
    val executable = Executable(Set(job5))
    
    val jobFilter = MockJobFilter(shouldRun = Set(job0, job3, job4), shouldNOTRun = Set(job1, job2, job5))
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    //NB: DryRunner is 'conservative'; if the JobFilter says a job should run, but subsequent jobs shouldn't, 
    //indicate that the subsequnt jobs will run since they follow on from one that does.
    //TODO: Improve this ordering
    assert(actual.map(_.name) === Seq(job0, job1, job2, job3, job4, job5).map(_.name))
  }
  
  test("toBeRun with JobFilter.RunEverything - more-complex topology, wide fanout") {
    /*    
     *     j1 (x)
     *    /   \
     * j0  ... +------ j100 (x)
     *   |\   /          |
     *   | j99 (x)       |
     *   |               |
     *   +---------------+
     */
    
    val job0 = mockJob("j0")
    val jobs1to99 = (1 to 99).map(i => mockJob(s"j$i", job0))
    val job100 = mockJob("j100", (job0 +: jobs1to99): _*)
    
    assert(jobs1to99.sortBy(_.id.toInt) === jobs1to99)
    
    val executable = Executable(Set(job100))
    
    val jobFilter = JobFilter.RunEverything
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    //NB: DryRunner is 'conservative'; if the JobFilter says a job should run, but subsequent jobs shouldn't, 
    //indicate that the subsequnt jobs will run since they follow on from one that does.
    assert(actual.to(Set) === (job0 +: jobs1to99 :+ job100).to(Set))
  }
  
  test("toBeRun with ByNameJobFilter - more-complex topology, wide fanout") {
    /*    
     *     j1 (x)
     *    /   \
     * j0  ... +------ j100 (x)
     *   |\   /          |
     *   | j99 (x)       |
     *   |               |
     *   +---------------+
     */
    
    val job0 = mockJob("j0")
    val jobs1to99 = (1 to 99).map(i => mockJob(s"j$i", job0))
    val job100 = mockJob("j100", (job0 +: jobs1to99): _*)
    
    assert(jobs1to99.sortBy(_.id.toInt) === jobs1to99)
    
    val executable = Executable(Set(job100))
    
    val jobFilter = ByNameJobFilter.anyOf("j0".r, "j4\\d".r)
    
    val actual = DryRunner.toBeRun(jobFilter, executable)
    
    //NB: In this case, dependency relationships are ignored, and what the ByNameJobFilter says goes.
    assert(actual.map(_.name).to(Set) === Set("j0","j40","j41","j42","j43","j44","j45","j46","j47","j48","j49"))
  }
}

object DryRunnerTest {
  private final case class MockJobFilter(
      shouldRun: Set[LJob] = Set.empty, 
      shouldNOTRun: Set[LJob] = Set.empty) extends JobFilter {
    
    require(shouldRun.intersect(shouldNOTRun).isEmpty)
    
    override def shouldRun(job: LJob): Boolean = shouldRun.contains(job) || !shouldNOTRun.contains(job)
  }
}
