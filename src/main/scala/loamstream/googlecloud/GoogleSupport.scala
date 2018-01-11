package loamstream.googlecloud

import loamstream.model.Store
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamCmdTool

object GoogleSupport {
  def googleCopy(
      srcs: Iterable[Store], 
      dests: Iterable[Store], 
      params: String*)(implicit context: LoamScriptContext): Unit = {
    
    for((src, dest) <- srcs.zip(dests)) {
      googleCopy(src, dest, params: _*)
    }
}

  def googleCopy(src: Store, dest: Store, params: String*)(implicit context: LoamScriptContext): Unit = {
    import LoamCmdTool._
    
    val googleConfig = context.googleConfig
    
    val gsutil =  googleConfig.gsutilBinary.toAbsolutePath
    
    cmd"""${gsutil} cp ${params.mkString(" ")} ${src} ${dest}""".in(src).out(dest)
  }
}
