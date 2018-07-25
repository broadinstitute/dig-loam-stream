package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.model.jobs.JobNode
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.util.Futures
import loamstream.util.Observables
import rx.lang.scala.Observable

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
    import loamstream.TestHelpers.waitFor
    
    val actualNames: Seq[String] = waitFor(namesObservable.to[Seq].firstAsFuture).sorted
    
    assert(actualNames === Seq("A", "A", "A", "B", "B", "B"))
  }
}
