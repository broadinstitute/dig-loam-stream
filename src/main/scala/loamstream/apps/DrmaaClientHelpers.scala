package loamstream.apps

import loamstream.uger.DrmaaClient
import loamstream.uger.Drmaa1Client
import loamstream.util.Loggable
import org.ggf.drmaa.DrmaaException
import loamstream.uger.UgerClient

/**
 * @author clint
 * date: Jul 5, 2016
 * 
 * Helper methods for managing the lifecycle of DRMAA Clients
 */
trait DrmaaClientHelpers extends Loggable {
  private[apps] def makeDrmaaClient(ugerClient: UgerClient): DrmaaClient = new Drmaa1Client(ugerClient)
  
  private[apps] def withClient(ugerClient: UgerClient)(f: DrmaaClient => Unit): Unit = {
    val drmaaClient = makeDrmaaClient(ugerClient)
    
    try { f(drmaaClient) }
    catch {
      case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
    }
    finally { drmaaClient.stop() }
  }
}
