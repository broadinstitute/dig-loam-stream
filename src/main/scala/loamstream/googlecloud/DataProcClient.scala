package loamstream.googlecloud

import loamstream.util.Terminable
import scala.concurrent.Future
import loamstream.util.Futures
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * Nov 28, 2016
 */
trait DataProcClient {
  def startCluster(): Unit
  
  def deleteCluster(): Unit
  
  def isClusterRunning: Boolean
  
  final def deleteClusterIfRunning(): Unit = {
    if(isClusterRunning) {
      deleteCluster()
    }
  }
  
  final def doWithCluster[A](f: => Future[A])(implicit context: ExecutionContext): Future[A] = {
    startCluster()
      
    import Futures.Implicits._
    
    f.withSideEffect(_ => deleteClusterIfRunning())
  }
}