package loamstream.dsl

import loamstream.LEnv
import loamstream.dsl.ToolBuilder.{StoreToken, Token}
import loamstream.model.LId

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object ToolBuilder {

  trait Token

  case class StringToken(string: String) extends Token {
    def +(oStringToken: StringToken): StringToken = StringToken(string + oStringToken.string)

    override def toString: String = string
  }

  case class EnvToken(key: LEnv.KeyBase) extends Token {
    override def toString: String = s"env[${key.tpe}]"
  }

  case class StoreToken(store: StoreBuilder) extends Token {
    override def toString: String = store.toString
  }

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
    def cmd(args: Any*)(implicit flowBuilder: FlowBuilder): ToolBuilder = {
      val stringPartsIter = stringContext.parts.iterator
      val argsIter = args.iterator
      var tokens: Seq[Token] = Seq(StringToken(stringPartsIter.next))
      while (stringPartsIter.hasNext) {
        val arg = argsIter.next()
        val argToken = arg match {
          case key: LEnv.KeyBase => EnvToken(key)
          case store: StoreBuilder => StoreToken(store)
          case _ => StringToken(arg.toString)
        }
        tokens :+= argToken
        tokens :+= StringToken(stringPartsIter.next())
      }
      tokens = mergeStringTokens(tokens)
      ToolBuilder(LId.newAnonId, tokens)
    }
  }

}

case class ToolBuilder(id: LId, tokens: Seq[Token])(implicit flowBuilder: FlowBuilder) {
  update()

  def update(): Unit = flowBuilder.add(this)

  def stores : Seq[StoreBuilder] = tokens.collect({case StoreToken(store) => store})

  override def toString: String = tokens.mkString
}