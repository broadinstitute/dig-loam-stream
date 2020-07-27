package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try

import org.ggf.drmaa.Session
import org.ggf.drmaa.SessionFactory

import loamstream.conf.DrmConfig
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Loops
import loamstream.util.Terminable

/**
 * @author clint
 * Jul 13, 2020
 */
trait SessionSource extends Terminable {
  def getSession: String
}

object SessionSource extends Loggable {
  object Noop extends SessionSource {
    override def getSession: String = ""
    
    override def stop(): Unit = ()
  }
}