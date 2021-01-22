package loamstream.model.execute

/**
 * @author clint
 * Dec 18, 2020
 */
final case class InterruptedExecutionException(
    executionState: Iterable[JobExecutionState], 
    message: String,
    cause: Throwable = null //scalastyle:ignore null
    ) extends Exception(message, cause)
