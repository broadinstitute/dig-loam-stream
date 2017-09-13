package loamstream.loam

import java.nio.file.Paths

import loamstream.loam.files.LoamFileManager
import loamstream.model.Store

/**
  * LoamStream
  * Created by oliverr on 6/14/2016.
  */
sealed trait LoamToken {
  def toString(fileManager: LoamFileManager): String
}

object LoamToken {
  def storesFromTokens(tokens: Seq[LoamToken]): Set[Store.Untyped] = {
    tokens.collect {
      case StoreToken(store) => store
      case StoreRefToken(storeRef) => storeRef.store
    }.toSet
  }

  final case class StringToken(string: String) extends LoamToken {
    def +(oStringToken: StringToken): StringToken = StringToken(string + oStringToken.string)

    override def toString: String = string

    override def toString(fileManager: LoamFileManager): String = toString
  }

  final case class StoreToken(store: Store.Untyped) extends LoamToken {
    override def toString: String = store.toString

    override def toString(fileManager: LoamFileManager): String = store.render(fileManager)
  }
  
  final case class StoreRefToken(storeRef: LoamStoreRef) extends LoamToken {
    override def toString: String = storeRef.pathModifier(storeRef.store.path).toString

    override def toString(fileManager: LoamFileManager): String = storeRef.render(fileManager)
  }
  
  final case class MultiStoreToken(stores: Iterable[HasLocation]) extends LoamToken {
    override def toString: String = stores.map(_.path).mkString(" ")

    override def toString(fileManager: LoamFileManager): String = {
      stores.map(_.render(fileManager)).mkString(" ")
    }
  }
  
  final case class MultiToken[A](as: Iterable[A]) extends LoamToken {
    override def toString: String = as.mkString(" ")

    override def toString(fileManager: LoamFileManager): String = toString
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

