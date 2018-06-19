package loamstream.loam

import java.nio.file.Paths

import loamstream.model.Store
import loamstream.model.execute.Locations
import java.nio.file.Path
import loamstream.model.UriStore
import loamstream.model.PathStore
import scala.reflect.ClassTag

/**
  * LoamStream
  * Created by oliverr on 6/14/2016.
  */
sealed trait LoamToken {
  def render: String
  
  override def toString = render
}

object LoamToken {
  
  def storesFromTokens(tokens: Seq[LoamToken]): Set[Store] = {
    tokens.collect { case StoreToken(store, _) => store }.toSet
  }

  final case class StringToken(string: String) extends LoamToken {
    def +(oStringToken: StringToken): StringToken = StringToken(string + oStringToken.string)

    override def render: String = string
  }

  final case class StoreToken(
      store: Store, 
      pathLocations: Locations[Path]) extends LoamToken {
    override def toString: String = store.toString

    override def render: String = store match {
      case us: UriStore => us.render
      case PathStore(_, p) => Store.render(pathLocations.inContainer(p))
    }
    
    override def hashCode: Int = store.hashCode
    
    override def equals(other: Any): Boolean = other match {
      case that: StoreToken => this.store == that.store
      case _ => false
    }
  }
  
  final case class MultiStoreToken(
      stores: Iterable[Store],
      pathLocations: Locations[Path]) extends LoamToken {
    
    override def render: String = stores.map {
      case us: UriStore => us.render
      case PathStore(_, p) => Store.render(pathLocations.inContainer(p))
    }.mkString(" ")
    
    override def hashCode: Int = stores.hashCode
    
    override def equals(other: Any): Boolean = other match {
      case that: MultiStoreToken => this.stores == that.stores
      case _ => false
    }
  }
  
  final case class MultiToken[A](as: Iterable[A]) extends LoamToken {
    override def render: String = as.mkString(" ")
  }

  def mergeStringTokens(tokens: Seq[LoamToken]): Seq[LoamToken] = {
    val tokenIter = tokens.iterator.filter {
      case StringToken(string) => string.nonEmpty
      case _ => true
    }

    if(tokenIter.isEmpty) {
      Nil
    } else {
      val z: (Seq[LoamToken], LoamToken) = (Seq.empty, tokenIter.next())
      
      val (merged, last) = tokenIter.foldLeft(z) { (acc, nextToken) =>
        val (mergedSoFar, current) = acc
        
        (current, nextToken) match {
          case (currentStringToken: StringToken, nextStringToken: StringToken) =>
            (mergedSoFar, currentStringToken + nextStringToken)
          case _ => (mergedSoFar :+ current, nextToken)
        }
      }
      
      merged :+ last
    }
  }
}

