package loamstream.googlecloud

import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamScriptContext
import loamstream.loam.LanguageSupport

/**
 * @author clint
 * Feb 17, 2017
 */
object HailSupport {
  /**
   * Provides a hail"..." interpolator that invokes cmd"..." but with Google Cloud boilerplate prepended; 
   * Gets Google Cloud and Hail config info from scriptContext.projectContext.config .
   */
  implicit final class StringContextWithHail(val stringContext: StringContext) extends AnyVal {
    def pyhail(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      import LanguageSupport.{determineScriptFile, makeScriptContent, makeScript, GeneratedScriptParams}
      
      val scriptFile = makeScript(stringContext, GeneratedScriptParams("pyhail", "py", hailConfig.scriptDir), args: _*)
      
      hail"$scriptFile"
    }
    
    def hail(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      
      require(
          scriptContext.executionEnvironment.isGoogle, 
          """hail"..." interpolators must be in google { ... } blocks""")
      
      val jarFile = hailConfig.jarFile
      
      val googlePrefixParts: Seq[String] = Seq(
          "", 
          /*${googleConfig.gcloudBinary}, */
          " dataproc jobs submit pyspark --cluster=", 
          /*${googleConfig.clusterId}*/ 
          " --files=",
          /*${hailConfig.jar}*/
          " --py-files=",
          /*${hailConfig.zip}*/
          s""" --properties="spark.driver.extraClassPath=./$jarFile,spark.executor.extraClassPath=./$jarFile" """)
          
      val googlePrefixArgs: Seq[Any] = Seq(
          googleConfig.gcloudBinary, 
          googleConfig.clusterId, 
          hailConfig.jar, 
          hailConfig.zip)
      
      //NB: Combine last google prefix part with first user-provided part, if the latter exists.  This matches what
      //the compiler would provide.
      val newParts: Seq[String] = stringContext.parts.toSeq match {
        case Nil => googlePrefixParts
        case firstPart +: otherParts => {
          googlePrefixParts.init ++ (s"${googlePrefixParts.last}${firstPart}" +: otherParts)
        }
      }
      
      val newArgs = googlePrefixArgs ++ args
      
      LoamCmdTool.StringContextWithCmd(StringContext(newParts: _*)).cmd(newArgs: _*)
    }
  }
  
  import LanguageSupport.config  
    
  private def hailConfig(implicit scriptContext: LoamScriptContext) = {
    require(
        config.hailConfig.isDefined,
        s"Hail support requires a valid 'loamstream.googlecloud.hail' section in the config file")
    
    config.hailConfig.get
  }
  
  private def googleConfig(implicit scriptContext: LoamScriptContext) = {
    require(
        config.googleConfig.isDefined, 
        s"Hail support requires a valid 'loamstream.googlecloud' section in the config file")
    
    config.googleConfig.get
  }
}
