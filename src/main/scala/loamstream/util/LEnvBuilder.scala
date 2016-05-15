package loamstream.util

import loamstream.LEnv

/**
  * LoamStream
  * Created by oliverr on 5/15/2016.
  */
object LEnvBuilder {
  val nameOfCLass = "loamstream.util.LEnvBuilder"
}

class LEnvBuilder {

  var env = LEnv.empty

  def +[V](entry: LEnv.Entry[V]): LEnvBuilder = {
    env = env + entry
    this
  }

  def +=[V](entry: LEnv.Entry[V]): LEnvBuilder = {
    env = env + entry
    this
  }

  def toEnv: LEnv = env

}
