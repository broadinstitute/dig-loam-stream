package loamstream.loam

import loamstream.conf.DynamicConfig
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.model.LId
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.Tool.AllStores
import loamstream.model.Tool.DefaultStores
import loamstream.util.StringUtils

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object LoamCmdTool extends LoamCmdSyntax {
  def toString(tokens: Seq[LoamToken]): String = tokens.map(_.render).mkString
}

/** A command line tool specified in a Loam script */
final case class LoamCmdTool private (
    val id: LId, 
    val tokens: Seq[LoamToken])(implicit val scriptContext: LoamScriptContext) extends Tool {

  /** Input and output stores before any are specified using in or out */
  override def defaultStores: DefaultStores = AllStores(LoamToken.storesFromTokens(tokens))

  /** Constructs the command line string */
  def commandLine: String = LoamCmdTool.toString(tokens)
}
