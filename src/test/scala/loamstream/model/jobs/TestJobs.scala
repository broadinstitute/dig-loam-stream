package loamstream.model.jobs

/**
 * @author clint
 * date: Jun 2, 2016
 */
trait TestJobs {
  protected val two0Success: JobStatus = JobStatus.Succeeded
  protected val two1Success: JobStatus = JobStatus.Succeeded

  protected val twoPlusTwoSuccess: JobStatus = JobStatus.Succeeded

  protected val plusOneSuccess: JobStatus = JobStatus.Succeeded
  
  protected val two0Failure: JobStatus = JobStatus.Failed
  protected val two1Failure: JobStatus = JobStatus.Failed

  protected val twoPlusTwoFailure: JobStatus = JobStatus.Failed

  protected val plusOneFailure: JobStatus = JobStatus.Failed

  protected val two0 = MockJob(two0Success)
  protected val two1 = MockJob(two1Success)

  protected val twoPlusTwo = MockJob(twoPlusTwoSuccess, inputs = Set[JobNode](two0, two1))

  protected val plusOne = MockJob(plusOneSuccess, inputs = Set[JobNode](twoPlusTwo))
  
  protected val two0Failed = MockJob(two0Failure)
  protected val two1Failed = MockJob(two1Failure)

  protected val twoPlusTwoFailed = MockJob(twoPlusTwoFailure, inputs = Set[JobNode](two0Failed, two1Failed))

  protected val plusOneFailed = MockJob(plusOneFailure, inputs = Set[JobNode](twoPlusTwoFailed))
}
