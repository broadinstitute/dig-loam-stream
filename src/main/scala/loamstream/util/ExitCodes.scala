package loamstream.util

/**
 * @author clint
 * Mar 30, 2017
 */
object ExitCodes {
  val success: Int = 0
  
  def isSuccess(exitCode: Int): Boolean = exitCode == success
  
  def isFailure(exitCode: Int): Boolean = !isSuccess(exitCode)
  
  def throwIfFailure(exitCode: Int, makeException: (Int, String) => Throwable = newException): Unit = {
    if(isFailure(exitCode)) {
      throw makeException(exitCode, s"Exit code '$exitCode' indicates failure")
    }
  }
  
  private def newException(exitCode: Int, message: String): ExitCodeException = ExitCodeException(exitCode, message)
}
