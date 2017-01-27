package loamstream.googlecloud

/**
 * @author clint
 * Nov 28, 2016
 */
trait DataProcClient {
  def startCluster(): Unit
  
  def deleteCluster(): Unit
  
  def isClusterRunning: Boolean
}