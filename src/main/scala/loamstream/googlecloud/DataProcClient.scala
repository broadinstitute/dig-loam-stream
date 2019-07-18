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
    def deleteCluster(): Unit
  }
  
  trait CanTellIfRunning {
    def isClusterRunning: Boolean
  }
  
  type CanStopAndTellIfRunning = CanStop with CanTellIfRunning
  
  private final class FromComponents(
      canStart: CanStart, 
      canStop: CanStop, 
      canTellIfRunning: CanTellIfRunning) extends DataProcClient {
    
    override def startCluster(): Unit = canStart.startCluster() 
  
    override def deleteCluster(): Unit = canStop.deleteCluster()
  
    override def isClusterRunning: Boolean = canTellIfRunning.isClusterRunning
  }
  
  def from(canStart: CanStart, canStop: CanStop, canTellIfRunning: CanTellIfRunning): DataProcClient = {
    new FromComponents(canStart, canStop, canTellIfRunning)
  }
}
