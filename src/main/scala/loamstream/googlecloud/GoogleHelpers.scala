package loamstream.googlecloud

import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamScriptContext
import loamstream.model.Store
import loamstream.util.BashScript.Implicits._

/**
 * @author clint
 * Jul 31, 2019
 */
trait GoogleHelpers {
  def googleCopy(
      srcs: Iterable[Store],
      dests: Iterable[Store],
      params: String*)(implicit context: LoamScriptContext): Iterable[LoamCmdTool] = {

    for((src, dest) <- srcs.zip(dests)) yield {
      googleCopy(src, dest, params: _*)
    }
  }

  def googleCopy(src: Store, dest: Store, params: String*)(implicit context: LoamScriptContext): LoamCmdTool = {
    import loamstream.loam.LoamCmdSyntax._

    val googleConfig = context.googleConfig

    val gsutil =  googleConfig.gsutilBinary.toAbsolutePath

    cmd"""${gsutil.render} cp ${params.mkString(" ")} ${src} ${dest}""".in(src).out(dest)
  }
}
