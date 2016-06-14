package loamstream.loam

import loamstream.LEnv
import loamstream.loam.LoamToken.{EnvToken, StoreToken, StringToken}
import loamstream.model.LId

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object LoamTool {

  implicit class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    def cmd(args: Any*)(implicit graphBuilder: LoamGraphBuilder): LoamTool = {
      val stringPartsIter = stringContext.parts.iterator
      val argsIter = args.iterator
      var tokens: Seq[LoamToken] = Seq(StringToken(stringPartsIter.next))
      while (stringPartsIter.hasNext) {
        val arg = argsIter.next()
        val argToken = arg match {
          case key: LEnv.KeyBase => EnvToken(key)
          case store: LoamStore => StoreToken(store)
          case _ => StringToken(arg.toString)
        }
        tokens :+= argToken
        tokens :+= StringToken(stringPartsIter.next())
      }
      tokens = LoamToken.mergeStringTokens(tokens)
      LoamTool.create(tokens)
    }
  }

  def create(tokens: Seq[LoamToken])(implicit graphBuilder: LoamGraphBuilder): LoamTool =
    graphBuilder.addTool(LoamTool(LId.newAnonId), tokens)

}

case class LoamTool private(id: LId)(implicit val graphBuilder: LoamGraphBuilder)