package loamstream.util

import scala.reflect.ClassTag

import HeterogeneousMap.Entry
import HeterogeneousMap.Key

/**
 * @author clint
 * Nov 6, 2019
 */
final class HeterogeneousMap private (delegate: Map[Key[_, _], Any]) {
  
  def +[K, V](entry: Entry[K, V]): HeterogeneousMap = new HeterogeneousMap(delegate + entry.toTuple)
  
  def ++(tuples: Iterable[Entry[_, _]]): HeterogeneousMap = tuples.foldLeft(this)(_ + _)
  
  def apply[K, V](key: Key[K, V]): V = get(key).get
  
  def get[K, V](key: Key[K, V]): Option[V] = {
    delegate.get(key).map(key.castValue)
  }
  
  def foreach(f: ((Any, Any)) => Any): Unit = delegate.foreach { case (Key(k), v) => f(k -> v) }
  
  def isEmpty: Boolean = delegate.isEmpty
  
  def size: Int = delegate.size
  
  def contains(key: Key[_, _]): Boolean = delegate.contains(key)
}

object HeterogeneousMap extends App {
  def apply(tuples: Entry[_, _]*): HeterogeneousMap = empty ++ tuples
  
  def empty: HeterogeneousMap = Empty
  
  private lazy val Empty: HeterogeneousMap = new HeterogeneousMap(Map.empty) 
  
  final case class Entry[K, V](key: Key[K, V], value: V) {
    def toTuple: (Key[K, V], V) = key -> value
  }
  
  final case class Key[K, V](key: K)(implicit classTagOfV: ClassTag[V]) {
    private[HeterogeneousMap] def castValue(a: Any): V = a match {
      case classTagOfV(v) => v
      case x => sys.error(s"Expected a ${classTagOfV.runtimeClass.getName} but got a ${x.getClass.getName}")
    }
    
    def ~>(v: V): Entry[K, V] = Entry(this, v)
  }
  
  final class KeyMaker[V : ClassTag] {
    def of[K](k: K): Key[K, V] = Key(k)
  }
  
  def keyFor[V : ClassTag]: KeyMaker[V] = new KeyMaker
}
