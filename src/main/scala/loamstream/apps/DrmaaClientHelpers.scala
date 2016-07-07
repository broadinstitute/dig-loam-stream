package loamstream.apps

import loamstream.uger.DrmaaClient
import loamstream.uger.Drmaa1Client

/**
 * @author clint
 * date: Jul 5, 2016
 * 
 * Helper methods for managing the lifecycle of DRMAA Clients
 */
trait DrmaaClientHelpers {
  protected def withClient[A](f: DrmaaClient => A): A = {
    val drmaaClient = DrmaaClient.drmaa1(new Drmaa1Client)
    
    try { f(drmaaClient) } 
    finally { drmaaClient.shutdown() }
  }
}