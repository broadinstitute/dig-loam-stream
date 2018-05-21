package loamstream.drm.lsf

import scala.util.Try
import loamstream.util.Loggable
import loamstream.util.Terminable

/**
 * @author clint
 * May 21, 2018
 */
object InvokesBjobs {
  type InvocationFn[A] = A => Try[RunResults]
  
  abstract class Companion[P, A](constructor: InvocationFn[P] => A) extends Loggable {
    
    private[lsf] def makeTokens(actualExecutable: String, params: P): Seq[String]
    
    final def fromExecutable(actualExecutable: String = "bjobs"): A = {
      def invocationFn(lsfJobIds: P): Try[RunResults] = {
        val tokens = makeTokens(actualExecutable, lsfJobIds)
        
        import scala.sys.process._
        
        debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
        
        //NB: Implicit conversion to ProcessBuilder :\
        val processBuilder: ProcessBuilder = tokens
        
        Processes.runSync(actualExecutable, processBuilder)
      }
      
      constructor(invocationFn)
    }
  }
  
  
}
