package loamstream.googlecloud

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
  
  final def doWithCluster[A](f: => A): A = {
    try { 
      startCluster()
      
      f
    } finally {
      deleteClusterIfRunning()
    }
  }
}