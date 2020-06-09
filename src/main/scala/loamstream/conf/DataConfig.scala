package loamstream.conf

import java.nio.file.{Path, Paths}

import com.typesafe.config.Config
import loamstream.util.ConfigUtils

/**
 * @author kyuksel
 *         date: 5/11/17
 *
 * A thin wrapper around [[com.typesafe.config.Config]]
 * to provide a restricted set of APIs for the DSL.
 * The methods let exceptions through in cases of invalid
 * path/type specifications since the DSL user will likely not
 * want the pipeline to proceed in such cases.
 */
final case class DataConfig private(config: Config) {
  /**
   * @param path key in config (x.y.z)
   * @return true unless path has value 'null' or is missing entirely
   *
   * Empty array and object values (e.g. 'path = []') return true
   */
  def isDefined(path: String): Boolean = config.hasPath(path)

  def getStr(path: String): String = config.getString(path)

  def getInt(path: String): Int = config.getInt(path)

  /** Value must be one of {true, false} and is case-sensitive */
  def getBool(path: String): Boolean = config.getBoolean(path)
  
  def getObj(path: String): DataConfig = DataConfig(config.getConfig(path))

  import scala.jdk.CollectionConverters._

  def getObjList(path: String): Seq[DataConfig] = config.getConfigList(path).asScala.map(DataConfig(_)).toVector

  def getStrList(path: String): Seq[String] = config.getStringList(path).asScala.toVector

  def getIntList(path: String): Seq[Int] = config.getIntList(path).asScala.map(_.toInt).toVector
}

object DataConfig {
  def fromFile(file: Path): DataConfig = {
    require(file.toFile.exists, s"Couldn't load config from non-existent file '$file'")
    
    DataConfig(ConfigUtils.configFromFile(file))
  }

  def fromFile(file: String): DataConfig = fromFile(Paths.get(file))
}
