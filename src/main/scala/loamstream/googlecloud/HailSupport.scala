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
import java.nio.file.Paths
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamToolBox
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.conf.LoamConfig

/**
 * @author clint
 * Feb 17, 2017
 */
object HailSupport extends App {
  def hail(hailParams: String)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
    val googleCloudHailPrefix = {
      require(scriptContext.projectContext.config.googleConfig.isDefined) //TODO
      require(scriptContext.projectContext.config.hailConfig.isDefined) //TODO
      
      val googleConfig = scriptContext.projectContext.config.googleConfig.get
      val hailConfig = scriptContext.projectContext.config.hailConfig.get
      
      s"""${googleConfig.gcloudBinary} dataproc jobs submit spark --cluster ${googleConfig.clusterId} 
          | --jar ${hailConfig.hailJar} --class org.broadinstitute.hail.driver.Main -- """.stripMargin
    }
    
    import LoamCmdTool._
    
    cmd"$googleCloudHailPrefix $hailParams"
  }
  
  private implicit val googleConfig = {
    GoogleCloudConfig(
        gcloudBinary = Paths.get("./gcloud"),
        projectId = "some-project",
        clusterId = "some-cluster",
        credentialsFile = Paths.get("./cred-file"))
  }
  
  private implicit val hailConfig = HailConfig(new URI("gs:/foo/bar/baz.jar"))
  
  private val loamConfig = LoamConfig(None, Some(googleConfig), Some(hailConfig))
  
  private implicit val scriptContext: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(loamConfig))
  
  println(hail("foo.py"))
  
  private val toolBox = new LoamToolBox(scriptContext.projectContext)
  
  private val job = toolBox.toolToJobShot(hail("foo.py")).get.asInstanceOf[CommandLineJob]
  
  println(job)
}
