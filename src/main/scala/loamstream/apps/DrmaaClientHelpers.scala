package loamstream.apps

import org.ggf.drmaa.DrmaaException

import loamstream.uger.Drmaa1Client
import loamstream.uger.DrmaaClient
import loamstream.util.Loggable

/**
 * @author clint
 * date: Jul 5, 2016
 * 
 * Helper methods for managing the lifecycle of DRMAA Clients
 */
trait DrmaaClientHelpers extends Loggable {
  private[apps] def makeDrmaaClient: DrmaaClient = new Drmaa1Client
  
  private[apps] def withClient(f: DrmaaClient => Unit): Unit = {
    val drmaaClient = makeDrmaaClient
    
    try { f(drmaaClient) }
    catch {
      case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
    }
    finally { drmaaClient.stop() }
  }
}
