package loamstream.googlecloud

import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamScriptContext

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
    def hail(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      
      def config = scriptContext.projectContext.config
      
      require(
          scriptContext.executionEnvironment.isGoogle, 
          """hail"..." interpolators must be in google { ... } blocks""")
      
      require(
          config.googleConfig.isDefined, 
          s"Hail support requires a valid 'loamstream.googlecloud' section in the config file")
          
      require(
          config.hailConfig.isDefined,
          s"Hail support requires a valid 'loamstream.googlecloud.hail' section in the config file")
      
      val googleConfig = scriptContext.projectContext.config.googleConfig.get
      val hailConfig = scriptContext.projectContext.config.hailConfig.get
      
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
}
