package loamstream.db.slick

import loamstream.model.jobs.JobStatus

/**
 * @author clint
 *         date: Sep 22, 2016
 */
final case class ExecutionRow(
    id: Int, 
    env: String, 
    cmd: Option[String], 
    status: JobStatus, 
    exitCode: Int,
    jobDir: Option[String],
    terminationReason: Option[String],
    runId: Option[Int] = None)
