package loamstream.model.execute

import java.nio.file.Paths

import loamstream.model.jobs.commandline.{CommandLineJob, CommandLineStringJob}
import org.scalatest.FunSuite

import scala.sys.process.ProcessLogger

/**
  * Created by kyuksel on 7/15/16.
  */
class ChunkedExecuterTest extends FunSuite {
  val curDir = Paths.get(".")
  var numExecutions: Int = 0
  val processLogger = ProcessLogger(line => recordStdOut(line))

  val firstStepJob = CommandLineStringJob("echo 1st_step_completed", curDir,
    Set.empty, CommandLineJob.mustBeZero, processLogger)
  val secondStepJob1 = CommandLineStringJob("echo 2nd_step_job_1_completed", curDir,
    Set(firstStepJob), CommandLineJob.mustBeZero, processLogger)
  val secondStepJob2 = CommandLineStringJob("echo 2nd_step_job_2_completed", curDir,
    Set(firstStepJob), CommandLineJob.mustBeZero, processLogger)
  val secondStepJob3 = CommandLineStringJob("echo 2nd_step_job_3_completed", curDir,
    Set(firstStepJob), CommandLineJob.mustBeZero, processLogger)
  val thirdStepJob = CommandLineStringJob("echo 3rd_step_job_completed", curDir,
    Set(secondStepJob1, secondStepJob2, secondStepJob3), CommandLineJob.mustBeZero, processLogger)

  val executer = ChunkedExecuter.default

  def recordStdOut(line: String): Unit = this.synchronized {
    numExecutions = numExecutions + 1
  }

  test("Parallel pipeline mocking imputation results in expected number of steps") {
    /* Two-step pipeline to result in 4 executions:
     *
     *           Impute0
     *          /
     * ShapeIt --Impute1
     *          \
     *           Impute2
     */
    val twoStepExecutable = LExecutable(Set(secondStepJob1, secondStepJob2, secondStepJob3))
    executer.execute(twoStepExecutable)
    assert(numExecutions === 6)

    /* Three-step pipeline to result in 5 more executions:
     *
     *           Impute0
     *          /       \
     * ShapeIt --Impute1--NoOp
     *          \       /
     *           Impute2
     */
    val threeStepExecutable = LExecutable(Set(thirdStepJob))
    executer.execute(threeStepExecutable)
    assert(numExecutions === 11)
  }
}
