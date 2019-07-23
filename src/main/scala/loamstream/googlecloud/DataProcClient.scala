package loamstream.googlecloud

/**
 * @author clint
 * Nov 28, 2016
 */
trait DataProcClient extends DataProcClient.CanStart with DataProcClient.CanStop with DataProcClient.CanTellIfRunning

object DataProcClient {
  trait CanStart {
    def startCluster(): Unit
  }
  
  trait CanStop {
    def stopCluster(): Unit
  }
  
  trait CanTellIfRunning {
    def isClusterRunning: Boolean
  }
  
  type CanStopAndTellIfRunning = CanStop with CanTellIfRunning
}
