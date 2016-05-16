package loamstream

import loamstream.LEnv.{Key, LComboEnv}
import loamstream.util.{LEnvBuilder, Shot, Snag}

/**
  * LoamStream
  * Created by oliverr on 3/30/2016.
  */
object LEnv {

  trait KeyBase {
    def name: String
  }

  type EntryBase = (KeyBase, Any)
  type Entry[V] = (Key[V], V)

  case class Key[V](name: String) extends KeyBase {
    def apply(value: V): Entry[V] = new Entry(this, value)

    def ->(value: V): Entry[V] = new Entry(this, value) // TODO remove this - ambiguous!

    def :=(value: V)(implicit envBuilder: LEnvBuilder): Entry[V] = {
      val entry = new Entry(this, value)
      envBuilder += entry
      entry
    }
  }

  def empty: LEnv = LMapEnv(Map.empty)

  def apply(entry: EntryBase, entries: EntryBase*): LMapEnv = LMapEnv((entry +: entries).toMap)

  case class LMapEnv(entries: Map[KeyBase, Any]) extends LEnv {
    override def apply[V](key: Key[V]): V = entries(key).asInstanceOf[V]

    override def get[V](key: Key[V]): Option[V] = entries.get(key).map(_.asInstanceOf[V])

    override def +[V](key: Key[V], value: V): LEnv = copy(entries = entries + (key -> value))
  }

  object LComboEnv {
    def apply(env: LEnv, envs: LEnv*): LComboEnv = LComboEnv(env +: envs)
  }

  case class LComboEnv(envs: Seq[LEnv]) extends LEnv {
    override def apply[V](key: Key[V]): V = envs.flatMap(_.get(key)).head

    override def get[V](key: Key[V]): Option[V] = envs.flatMap(_.get(key)).headOption

    override def +[V](key: Key[V], value: V): LEnv = envs.head match {
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
  def apply[V](key: Key[V]): V

  def get[V](key: Key[V]): Option[V]

  def shoot[V](key: Key[V]): Shot[V] = Shot.fromOption(get(key), Snag("No value for key " + key.name))

  def +[V](key: Key[V], value: V): LEnv

  def +[V](entry: (Key[V], V)): LEnv = this. +(entry._1, entry._2)

  def ++(oEnv: LEnv): LComboEnv = oEnv match {
    case LComboEnv(oEnvs) => LComboEnv(this +: oEnvs)
    case _ => LComboEnv(this, oEnv)
  }
}
