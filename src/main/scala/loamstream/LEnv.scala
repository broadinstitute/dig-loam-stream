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
}

object LEnv {

  trait KeyBase {
    def id: LId
  }

  type EntryBase = (KeyBase, Any)
  type Entry[V] = (Key[V], V)

  final case class Key[V](typeBox: TypeBox[V], id: LId) extends KeyBase {
  }

  object Key {
  }

}

