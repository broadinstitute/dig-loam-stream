package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.util.Futures
import loamstream.util.Observables
import monix.reactive.Observable
import loamstream.TestHelpers
import monix.execution.Scheduler


/**
 * @author clint
 * Oct 2, 2017
 */
final class ExecutableTest extends FunSuite {
  test("multiplex") {
    def names(howMany: Int)(jobNode: JobNode): Observable[String] = {
      Observable.from((1 to howMany).map(_ => jobNode.job.name))
    }
    
    import JobStatus.Succeeded
    
    val jobA = MockJob(Succeeded, "A")
    val jobB = MockJob(Succeeded, "B")

    val executable = Executable(Set(jobA, jobB))
    
    val namesObservable = executable.multiplex(names(3))
    
    import Observables.Implicits._
    import Scheduler.Implicits.global
    
    val actualNames: Seq[String] = namesObservable.toListL.runSyncUnsafe(TestHelpers.defaultWaitTime).sorted
    
    assert(actualNames === Seq("A", "A", "A", "B", "B", "B"))
  }
}
