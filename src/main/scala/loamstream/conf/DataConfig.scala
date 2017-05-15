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
  def getStr(path: String): String = config.getString(path)

  def getInt(path: String): Int = config.getInt(path)

  import scala.collection.JavaConverters._

  def getObjList(path: String): Seq[DataConfig] = config.getConfigList(path).asScala.map(DataConfig(_)).toVector

  def getStrList(path: String): Seq[String] = config.getStringList(path).asScala.toVector

  def getIntList(path: String): Seq[Int] = config.getIntList(path).asScala.map(_.toInt).toVector
}

object DataConfig {
  def fromFile(file: Path): DataConfig = DataConfig(ConfigUtils.configFromFile(file))

  def fromFile(file: String): DataConfig = fromFile(Paths.get(file))
}
