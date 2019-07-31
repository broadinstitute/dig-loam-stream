package loamstream.googlecloud

import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamScriptContext
import loamstream.loam.LanguageSupport

/**
 * @author clint
 * Feb 17, 2017
 */
trait HailLoamSyntax {
  /**
   * Provides a hail"..." interpolator that invokes cmd"..." but with Google Cloud boilerplate prepended; 
   * Gets Google Cloud and Hail config info from scriptContext.projectContext.config .
   */
  implicit final class StringContextWithHail(val stringContext: StringContext) {
    /**
     * Declare a Tool that will run Hail by specifying the Python driver script inline.  For example:
     * 
     * pyhail"""#driver script code
     *          #...
     *          #more Python code""" 
     */
    def pyhail(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      import LanguageSupport.{makeScript, GeneratedScriptParams}
      
      val hailConfig = scriptContext.hailConfig
      
      val scriptParams = GeneratedScriptParams("pyhail", "py", hailConfig.scriptDir)
      
      val scriptFile = makeScript(stringContext, scriptParams, args: _*)
      
      hail"$scriptFile"
    }
    
    /**
     * Declare a Tool that will run Hail by specifying Hail command-line args.  Usually, this will start with a 
     * driver script file, like
     * 
     * val projectId = "foo"
     * val inputVcf = store(uri("gs://foo/bar/baz.vcf"))
     * val outputVds = store(uri("gs://foo/bar/baz.vds")) 
     * 
     * hail"""some_driver_file.py
     *        --vcf-in $projectId ${inputVcf}
     *        --vds-out ${outputVds}"""
     *        
     * This Tool will invoke `gcloud` to submit a Hail job to Google Cloud Dataproc.  Boilerplate params will be
     * prepended to the contents of the hail"..." string, and $-variables will be interpolated.  This will result in
     * a command line something like
     * 
     * `gcloud dataproc jobs submit pyspark --cluster=<cluster id from config file> --files=<URI of Hail jar> \
     *  --py-files=<URI of Hail zip file> --properties="spark.driver.extraClassPath=<URI of Hail jar>,\
     *  spark.executor.extraClassPath=<URI of Hail jar> \
     *  some_driver_file.py \
     *  --vcf-in foo gs://foo/bar/baz.vcf \
     *  --vds-out gs://foo/bar/baz.vds`
     *  
     * being run.
     */
    def hail(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
      
      require(
          scriptContext.settings.isGoogle, 
          """hail"..." interpolators must be in google { ... } blocks""")
      
      val hailConfig = scriptContext.hailConfig
      val googleConfig = scriptContext.googleConfig
          
      val hailctlPrefixPart: String = {
        import loamstream.util.Paths.normalize
        
        val sourceBashrcPart = "source ~/.bashrc"
        val activateCondaPart = s"""conda activate "${hailConfig.condaEnv}""""
        val projectIdEnvVarPart = s"""CLOUDSDK_CORE_PROJECT="${googleConfig.projectId}""""
        val pathMungingPart = s"""PATH="${normalize(googleConfig.gcloudBinary.getParent)}":$${PATH}"""
    
        val prefix = s"${sourceBashrcPart} && ${activateCondaPart} && ${projectIdEnvVarPart} && ${pathMungingPart}"
        
        s"""${prefix} && hailctl dataproc submit ${googleConfig.clusterId} """
      }
          
      //NB: Combine prefix part with first user-provided part, if the latter exists.  This matches what
      //the compiler would provide.
      val newParts: Seq[String] = stringContext.parts.toSeq match {
        case Nil => Seq(hailctlPrefixPart)
        case firstPart +: otherParts => s"${hailctlPrefixPart}${firstPart}" +: otherParts
      }
      
      val newArgs = args
      
      LoamCmdTool.StringContextWithCmd(StringContext(newParts: _*)).cmd(newArgs: _*)
    }
  }
}
