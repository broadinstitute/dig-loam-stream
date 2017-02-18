package loamstream.googlecloud

import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamToken
import loamstream.loam.LoamStore
import loamstream.loam.LoamStoreRef
import loamstream.loam.LoamToken.StringToken
import loamstream.loam.LoamToken.StoreRefToken
import loamstream.loam.LoamToken.StoreToken
import java.nio.file.Path
import java.net.URI

/**
 * @author clint
 * Feb 17, 2017
 */
final case class HailSupport(gcloudBinary: Path, clusterId: String, hailJar: URI) {
  
   implicit final class StringContextWithHail(val stringContext: StringContext) {
    
    def hail(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      val oldParts = stringContext.parts
        
      val googleCloudHailPrefixParts = Seq(
          /* $gcloud, */
          " dataproc jobs submit spark --cluster ", /* $clusterId */
          "--jar ", /* $hailCloudJar */
          "--class org.broadinstitute.hail.driver.Main -- ")
      
      val newParts: Seq[String] = {
        if(oldParts.isEmpty) { googleCloudHailPrefixParts }
        else { googleCloudHailPrefixParts ++ stringContext.parts }
      }
      
      val newArgs = Seq(gcloudBinary, clusterId, hailJar) +: args
        
      LoamCmdTool.StringContextWithCmd(StringContext(newParts: _*)).cmd(newArgs: _*)
    }
  }
  
  
}

object HailSupport extends App {
  val hailSupport = HailSupport(???, ???, ???)
  
  import hailSupport._
  
  private implicit val scriptContext: LoamScriptContext = ???
  
  hail""
}
