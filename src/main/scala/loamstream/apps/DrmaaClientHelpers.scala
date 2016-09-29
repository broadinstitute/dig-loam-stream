package loamstream.apps

import loamstream.uger.DrmaaClient
import loamstream.uger.Drmaa1Client
import loamstream.util.Loggable
import org.ggf.drmaa.DrmaaException

/**
 * @author clint
 * date: Jul 5, 2016
 * 
 * Helper methods for managing the lifecycle of DRMAA Clients
 */
trait DrmaaClientHelpers extends Loggable {
  protected def withClient(f: DrmaaClient => Unit): Unit = {
    val drmaaClient = DrmaaClient.drmaa1(new Drmaa1Client)
    
    try { f(drmaaClient) }
    catch {
      case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
    }
    finally { drmaaClient.shutdown() }
  }
}