package loamstream.drm.lsf

import org.scalatest.FunSuite
import org.yaml.snakeyaml.Yaml
import java.io.StringReader
import loamstream.TestHelpers
import loamstream.util.BashScript
import java.nio.file.Path

/**
 * @author clint
 * Jun 18, 2018
 */
final class ContainerDefinitionFileTest extends FunSuite {
  import TestHelpers.path
  
  private val absoluteBase = path("/output/dir")
  private val image = "library/foo:123"
  private val username = System.getProperty("user.name")
  
  private def munge(p: Path): String = {
    import BashScript.Implicits._
    
    p.toAbsolutePath.render
  }
  
  test("toYaml - no mounted dirs") {
    
    val params = LsfDockerParams(imageName = image, Nil, outputDir = absoluteBase)
    
    val yaml = ContainerDefinitionFile(params, path("/some/file/we/wont/use/here")).toYaml
    
    val parsed = parse(yaml)

    assert(parsed("image") === image)
    assert(parsed("mount_home") === "false")
    assert(parsed("mounts") === s"[${munge(path("."))}]")
    assert(parsed("write_output") === "true")
    assert(parsed("output") === "/output/dir")
    assert(parsed("output_backend") === "output_hps_nobackup")
  }
  
  test("toYaml - with mounted dirs") {
    
    val mountedDirs = Seq(path("foo/bar"), path("/blerg/zerg"))
    
    val params = LsfDockerParams(imageName = image, mountedDirs = mountedDirs, outputDir = absoluteBase)
    
    val yaml = ContainerDefinitionFile(params, path("/some/file/we/wont/use/here")).toYaml
    
    val parsed = parse(yaml)
    
    assert(parsed("image") === image)
    assert(parsed("mount_home") === "false")
    assert(parsed("mounts") === s"[${mountedDirs.map(munge).mkString(", ")}]")
    assert(parsed("write_output") === "true")
    assert(parsed("output") === "/output/dir")
    assert(parsed("output_backend") === "output_hps_nobackup")
  }
  
  private def parse(yaml: String): Map[String, String] = {
    import java.{util => ju}
    
    val javaMap = (new Yaml).load(new StringReader(yaml)).asInstanceOf[ju.Map[String, Any]]
    
    import scala.collection.JavaConverters._
    
    javaMap.asScala.toMap.map { case (k, v) => (k, v.toString) }
  }
}
