package loamstream.dsl

import loamstream.LEnv
import loamstream.dsl.StringCommandBuilder.Token
import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object StringCommandBuilder {

  val command = new LEnv.Key[StringCommandBuilder]("command")

  trait Token

  case class StringToken(string: String) extends Token {
    def +(oStringToken: StringToken): StringToken = StringToken(string + oStringToken.string)
  }

  case class EnvToken(key: LEnv.KeyBase) extends Token

  trait Slot extends Token {
    def name: String
    def tpe: Type
  }

  case class InSlot(name: String, tpe:Type) extends Slot

  case class OutSlot(name: String, tpe: Type) extends Slot

  def mergeStringTokens(tokens: Seq[Token]): Seq[Token] = {
    var tokensMerged: Seq[Token] = Seq.empty
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

  implicit class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    def cmd(args: Any*): StringCommandBuilder = {
      val stringPartsIter = stringContext.parts.iterator
      val argsIter = args.iterator
      var tokens: Seq[Token] = Seq(StringToken(stringPartsIter.next))
      while (stringPartsIter.hasNext) {
        val arg = argsIter.next()
        val argToken = arg match {
          case key: LEnv.KeyBase => EnvToken(key)
          case InputBuilder(name, tpe) => InSlot(name, tpe)
          case OutputBuilder(name, tpe) => OutSlot(name, tpe)
          case _ => StringToken(arg.toString)
        }
        tokens :+= argToken
        tokens :+= StringToken(stringPartsIter.next())
      }
      tokens = mergeStringTokens(tokens)
      StringCommandBuilder(tokens)
    }
  }

}

case class StringCommandBuilder(tokens: Seq[Token])