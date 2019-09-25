package loamstream.model.jobs

import java.nio.file.Path
import loamstream.model.execute.EnvironmentType


/**
 * @author clint
 * Sep 12, 2019
 * 
 * A class representing an "Execution" retrieved from the DB.  Basically, an Execution without
 * settings or resources, which are not persisted. 
 */
final case class PseudoExecution(
    envType: EnvironmentType,
    cmd: Option[String] = None,
    status: JobStatus,
    result: Option[JobResult] = None,
    outputs: Set[StoreRecord] = Set.empty,
    jobDir: Option[Path],
    terminationReason: Option[TerminationReason]) extends Execution.Persisted
