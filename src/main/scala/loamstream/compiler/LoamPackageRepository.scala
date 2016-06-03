package loamstream.compiler

import java.io.File

import loamstream.util.{Shot, Snag}

import scala.io.{Codec, Source}
import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 6/3/2016.
  */
case class LoamPackageRepository(packageName: String) extends LoamRepository {
  override def get(name: String): Shot[String] = {
    val fullName = s"$packageName${File.separator}$name"
    val classLoader: ClassLoader = classOf[LoamPackageRepository].getClassLoader
    val iStreamShot =
      Shot.notNull(classLoader.getResourceAsStream(fullName), Snag(s"Could not find resource $fullName"))
    iStreamShot.flatMap(is => Shot.fromTry(Try({
      Source.fromInputStream(is)(Codec.UTF8).mkString
    })))
  }
}
