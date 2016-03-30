package loamstream

import loamstream.LEnv.{Key, LComboEnv, Value}

/**
  * LoamStream
  * Created by oliverr on 3/30/2016.
  */
object LEnv {

  trait KeyBase

  trait Key[V <: Value] extends KeyBase

  trait Value

  case class LMapEnv(entries: Map[KeyBase, Value]) extends LEnv {
    override def get[V <: Value](key: Key[V]): Option[V] = entries.get(key).map(_.asInstanceOf[V])

    override def +[V <: Value](key: Key[V], value: V): LEnv = copy(entries = entries + (key -> value))
  }

  object LComboEnv {
    def apply(env: LEnv, envs: LEnv*): LComboEnv = LComboEnv(env +: envs)
  }

  case class LComboEnv(envs: Seq[LEnv]) extends LEnv {
    override def apply[V <: Value](key: Key[V]): V = envs.flatMap(_.get(key)).head

    override def get[V <: Value](key: Key[V]): Option[V] = envs.flatMap(_.get(key)).headOption

    override def +[V <: Value](key: Key[V], value: V): LEnv = envs.head match {
      case mapEnv: LMapEnv => LComboEnv((mapEnv + (key -> value)) +: envs.tail)
      case _ => LComboEnv(LMapEnv(Map(key -> value)) +: envs)
    }

    override def ++(oEnv: LEnv): LComboEnv = oEnv match {
      case LComboEnv(oEnvs) => LComboEnv(envs ++ oEnvs)
      case _ => LComboEnv(envs :+ oEnv)
    }
  }

}

trait LEnv {
  def apply[V <: Value](key: Key[V]): V

  def get[V <: Value](key: Key[V]): Option[V]

  def +[V <: Value](key: Key[V], value: V): LEnv

  def +[V <: Value](entry: (Key[V], V)): LEnv = this + (entry._1, entry._2)

  def ++(oEnv: LEnv): LComboEnv = oEnv match {
    case LComboEnv(oEnvs) => LComboEnv(this +: oEnvs)
    case _ => LComboEnv(this, oEnv)
  }
}
