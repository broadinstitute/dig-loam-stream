package loamstream.conf

import java.io.FileInputStream
import java.nio.file.{DirectoryStream, Files, Path}
import java.util.Properties

import utils.FileUtils

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
  * LoamStream
  * Created by oliverr on 3/4/2016.
  */
object LProperties {

  def properties: Properties = System.getProperties

  val confFolderRelativePath = "/"

  val confFolderPath = FileUtils.resolveRelativePath(confFolderRelativePath)

  val propertyFilesFilter = new DirectoryStream.Filter[Path] {
    override def accept(file: Path): Boolean = file.toString.endsWith(".properties")
  }

  def propertyFilesIterator: Iterator[Path] =
    Files.newDirectoryStream(confFolderPath, propertyFilesFilter).iterator().asScala

  def propertyFilesIteratorUnfiltered: Iterator[Path] =
    Files.newDirectoryStream(confFolderPath).iterator().asScala

  def loadProperties(): Unit = {
    for (propertyFile <- propertyFilesIterator) {
      properties.load(new FileInputStream(propertyFile.toFile))
    }
  }

  loadProperties()

  def get(key: String): Option[String] = Option(properties.get(key)).map(_.asInstanceOf[String])
}
