package loamstream.model.execute

import java.lang.Boolean

import loamstream.model.execute.RxExecuter.{RxMockJob, Tracker}
import loamstream.model.jobs.{JobState, LJob, Output}
import loamstream.model.jobs.JobState.{NotStarted, Running, Succeeded}
import loamstream.model.jobs.LJob._
import loamstream.util._
import rx.lang.scala.subjects.PublishSubject

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.duration.Duration

/**
 * @author kaan
 *         date: Aug 17, 2016
 */
final class RxExecuter(val tracker: Tracker) extends Loggable {
  // scalastyle:off method.length
  def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    def flattenTree(tree: Set[LJob]): Set[LJob] = {
      tree.foldLeft(tree)((acc, x) =>
        x.inputs ++ flattenTree(x.inputs) ++ acc)
    }

    def getRunnableJobs(jobs: Set[LJob]): Set[LJob] = jobs.filter(_.isRunnable)

    // Mutable state variables
    val jobsAlreadyLaunched: ValueBox[Set[LJob]] = ValueBox(Set.empty)
    val jobsReadyToDispatch: ValueBox[Set[LJob]] = ValueBox(Set.empty)
    val jobStates: ValueBox[Map[LJob, JobState]] = ValueBox(Map.empty)
    val result: ValueBox[Map[LJob, Result]] = ValueBox(Map.empty)

    val allJobStatuses = PublishSubject[Map[LJob, JobState]]

    def updateJobState(job: LJob, newState: JobState): Unit = {
      jobStates.mutate(_ + (job -> newState))
      trace("+++Emitting all job statuses")
      allJobStatuses.onNext(jobStates())
    }

    // Future-Promise pair used as a flag to check if the main thread can be resumed (i.e. all jobs are done)
    val everythingIsDonePromise: Promise[Unit] = Promise()
    val everythingIsDoneFuture: Future[Unit] = everythingIsDonePromise.future

    def executeIter(jobs: Set[LJob]): Unit = {
      if (jobs.isEmpty) {
        if (jobStates().values.forall(_ == Succeeded)) {
          everythingIsDonePromise.success(())
        }
      } else {
        trace("Jobs already launched: ")
        jobsAlreadyLaunched().foreach(job => trace("\t" + job.asInstanceOf[RxMockJob].name))

        jobsReadyToDispatch() = getRunnableJobs(jobs) -- jobsAlreadyLaunched()

        debug("Jobs ready to dispatch: ")
        jobsReadyToDispatch().foreach(job => debug("\t" + job.asInstanceOf[RxMockJob].name))

        import scala.concurrent.ExecutionContext.Implicits.global
        Future {
          tracker.addJobs(jobsReadyToDispatch())
          jobsReadyToDispatch().par.foreach { job =>
            val newResult = Await.result(job.execute, Duration.Inf)
            val newResultMap = job -> newResult
            result.mutate(_ + newResultMap)
            jobsAlreadyLaunched.mutate(_ + job)
          }
        }
      }
    }

    val jobs = flattenTree(executable.jobs)

    import scala.language.postfixOps
    jobs foreach { job =>
      jobStates.mutate(_ + (job -> NotStarted))
      job.stateEmitter.subscribe(jobState => updateJobState(job, jobState))
    }

    allJobStatuses.sample(20 millis).subscribe(jobStatuses => {
      executeIter(getRunnableJobs(jobStatuses.keySet))
    })

    executeIter(jobs)

    // Block the main thread until all jobs are done
    Await.result(everythingIsDoneFuture, Duration.Inf)

    import Maps.Implicits._
    result().strictMapValues(Hit(_))
  }
  // scalastyle:off method.length
}

object RxExecuter {
  def default: RxExecuter = new RxExecuter(new Tracker)

  class RxMockJob(val name: String, val inputs: Set[LJob] = Set.empty, val outputs: Set[Output] = Set.empty,
                  override val dependencies: Set[LJob] = Set.empty, delay: Int = 0) extends LJob {

    override def print(indent: Int = 0, doPrint: String => Unit = debug(_)): Unit = {
      val indentString = s"${"-" * indent} >"

      doPrint(s"$indentString ${this}")

      inputs.foreach(_.print(indent + 2))
    }

    private def emitJobState: Unit = {
      trace(s"***Emitting state change for $name ==> " + state)
      stateEmitter.onNext(state)
    }

    private[this] val count = ValueBox(0)

    def executionCount = count.value

    def execute(implicit context: ExecutionContext): Future[Result] = Future {
      trace("\t\tStarting job: " + this.name)
      stateRef() = Running
      emitJobState
      if (delay > 0) { Thread.sleep(delay) }
      trace("\t\t\tFinishing job: " + this.name)
      stateRef() = Succeeded
      emitJobState
      count.mutate(_ + 1)
      LJob.SimpleSuccess(name)
    }

    def copy(
              name: String = this.name,
              inputs: Set[LJob] = this.inputs,
              outputs: Set[Output] = this.outputs,
              dependencies: Set[LJob] = this.dependencies,
              delay: Int = this.delay): RxMockJob = new RxMockJob(name, inputs, outputs, dependencies, delay)

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
  }

  final case class Tracker() {
    private val executionSeq: ValueBox[Array[Set[LJob]]] = ValueBox(Array.empty)

    def addJobs(jobs: Set[LJob]): Unit = executionSeq.mutate(_ :+ jobs)

    def jobExecutionSeq = executionSeq.value
  }

}