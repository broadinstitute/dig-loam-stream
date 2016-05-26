package loamstream.dsl

import loamstream.LEnv
import loamstream.dsl.StringCommandBuilder.Token

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object StringCommandBuilder {

  val command = new LEnv.Key[StringCommandBuilder]("command")

  trait Token

  case class StringToken(string: String) extends Token

  case class EnvToken(key: LEnv.KeyBase) extends Token

  trait Slot extends Token {
    def name: String
  }

  case class InSlot(name: String) extends Slot

  case class OutSlot(name: String) extends Slot

  implicit class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    def cmd(args: Any*): StringCommandBuilder = {
      val stringPartsIter = stringContext.parts.iterator
      val argsIter = args.iterator
      var tokens: Seq[Token] = Seq(StringToken(stringPartsIter.next))
      while (stringPartsIter.hasNext) {
        val arg = argsIter.next()
        val argToken = arg match {
          case key: LEnv.KeyBase => EnvToken(key)
          case InputBuilder(name) => InSlot(name)
          case OutputBuilder(name) => OutSlot(name)
          case _ => StringToken(arg.toString)
        }
        tokens :+= argToken
        tokens :+= StringToken(stringPartsIter.next())
      }
      StringCommandBuilder(tokens)
    }
  }

}

case class StringCommandBuilder(tokens: Seq[Token])