package loamstream.loam

import loamstream.LEnv

/**
  * LoamStream
  * Created by oliverr on 6/14/2016.
  */
sealed trait LoamToken

object LoamToken {
  def storesFromTokens(tokens: Seq[LoamToken]): Set[LoamStore] =
    tokens.collect({ case StoreToken(store) => store }).toSet

  case class StringToken(string: String) extends LoamToken {
    def +(oStringToken: StringToken): StringToken = StringToken(string + oStringToken.string)

    override def toString: String = string
  }

  case class EnvToken(key: LEnv.KeyBase) extends LoamToken {
    override def toString: String = s"env[${key.tpe}]"
  }

  case class StoreToken(store: LoamStore) extends LoamToken {
    override def toString: String = store.toString
  }

  def mergeStringTokens(tokens: Seq[LoamToken]): Seq[LoamToken] = {
    var tokensMerged: Seq[LoamToken] = Seq.empty
    val tokenIter = tokens.iterator.filter(_ match {
      case stringToken: StringToken if stringToken.string.length == 0 => false
      case _ => true
    })
    if (tokenIter.hasNext) {
      var currentToken = tokenIter.next()
      while (tokenIter.hasNext) {
        val nextToken = tokenIter.next()
        (currentToken, nextToken) match {
          case (currentStringToken: StringToken, nextStringToken: StringToken) =>
            currentToken = currentStringToken + nextStringToken
          case _ =>
            tokensMerged :+= currentToken
            currentToken = nextToken
        }
      }
      tokensMerged :+= currentToken
    }
    tokensMerged
  }
}

