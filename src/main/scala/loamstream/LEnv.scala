package loamstream

import loamstream.LEnv.{Key, KeyBase}
import loamstream.model.LId
import loamstream.util.TypeBox

import scala.reflect.runtime.universe.{Type, TypeTag}

/**
  * LoamStream
  * Created by oliverr on 3/30/2016.
  */
trait LEnv {
  def keys: Iterable[KeyBase]

  def apply[V](key: Key[V]): V

  def get[V](key: Key[V]): Option[V]

  def grab(key: KeyBase): Option[Any]

}

object LEnv {

  trait KeyBase {
    def id: LId

    def tpe: Type
  }

  type EntryBase = (KeyBase, Any)
  type Entry[V] = (Key[V], V)

  final case class Key[V](typeBox: TypeBox[V], id: LId) extends KeyBase {
    def tpe: Type = typeBox.tpe

    def apply(value: V): Entry[V] = new Entry(this, value)

    def ->(value: V): Entry[V] = new Entry(this, value) // TODO remove this - ambiguous!

  }

  object Key {
    def create[T: TypeTag]: Key[T] = apply[T](LId.newAnonId)

    def apply[T: TypeTag](id: LId): Key[T] = Key[T](TypeBox.of[T], id)
  }

  def empty: LEnv = LMapEnv(Map.empty)

  final case class LMapEnv(entries: Map[KeyBase, Any]) extends LEnv {
    override def keys: Set[KeyBase] = entries.keySet

    override def apply[V](key: Key[V]): V = entries(key).asInstanceOf[V]

    override def get[V](key: Key[V]): Option[V] = entries.get(key).map(_.asInstanceOf[V])

    override def grab(key: KeyBase): Option[Any] = entries.get(key)
  }

  final case class LComboEnv(envs: Seq[LEnv]) extends LEnv {
    override def keys: Iterable[KeyBase] = envs.flatMap(_.keys)

    override def apply[V](key: Key[V]): V = envs.flatMap(_.get(key)).head

    override def get[V](key: Key[V]): Option[V] = envs.flatMap(_.get(key)).headOption

    override def grab(key: KeyBase): Option[Any] = envs.flatMap(_.grab(key)).headOption
  }

}

