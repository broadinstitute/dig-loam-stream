package loamstream.loam

import loamstream.LEnv

/**
  * LoamStream
  * Created by oliverr on 5/15/2016.
  */
final class LEnvBuilder {

  @volatile private[this] var _env: LEnv = LEnv.empty

  private[this] val lock = new AnyRef
  
  private[this] def updateEnv(f: LEnv => LEnv): LEnvBuilder = lock.synchronized {
    _env = f(_env)
    
    this
  }
  
  def +[V](entry: LEnv.Entry[V]): LEnvBuilder = updateEnv(_ + entry)

  def +=[V](entry: LEnv.Entry[V]): LEnvBuilder = updateEnv(_ + entry)

  def toEnv: LEnv = lock.synchronized(_env)

}
