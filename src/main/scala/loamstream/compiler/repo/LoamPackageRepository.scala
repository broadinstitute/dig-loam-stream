package loamstream.compiler.repo

import java.io.File
import java.nio.file.{Path, Paths}

import loamstream.util.{Miss, Shot, Snag}

import scala.io.{Codec, Source}
import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 6/3/2016.
  */
case class LoamPackageRepository(packageName: String, entries: Seq[String]) extends LoamRepository {
  val classLoader: ClassLoader = classOf[LoamPackageRepository].getClassLoader

  def nameToFullName(name: String): String = s"$packageName${File.separator}$name${LoamRepository.fileSuffix}"

  override def get(name: String): Shot[String] = {
    val fullName = nameToFullName(name)
    val iStreamShot =
      Shot.notNull(classLoader.getResourceAsStream(fullName), Snag(s"Could not find resource $fullName"))
    iStreamShot.flatMap(is => Shot.fromTry(Try({
      Source.fromInputStream(is)(Codec.UTF8).mkString
    })))
  }

  override def list: Seq[String] = entries

  override def add(name: String, content: String): Shot[String] = {
    val urlTemplate = classLoader.getResource(nameToFullName(entries.head))
    val pathTemplate = urlTemplate.getPath
    val classFolder = Paths.get(pathTemplate).resolve("..")
    Miss(s"Not yet implemented how to deal with $classFolder.")
  }
}
