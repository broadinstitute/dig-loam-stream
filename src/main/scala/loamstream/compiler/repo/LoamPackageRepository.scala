package loamstream.compiler.repo

import java.io.File
import java.net.URL

import loamstream.compiler.messages.LoadResponseMessage
import loamstream.util.{Shot, Shots, Snag}

import scala.io.{Codec, Source}
import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 6/3/2016.
  */
case class LoamPackageRepository(packageName: String, entries: Seq[String]) extends LoamRepository {
  val classLoader: ClassLoader = classOf[LoamPackageRepository].getClassLoader

  def nameToFullName(name: String): String = s"$packageName${File.separator}$name${LoamRepository.fileSuffix}"

  override def load(name: String): Shot[LoadResponseMessage] = {
    val fullName = nameToFullName(name)
    val iStreamShot =
      Shot.notNull(classLoader.getResourceAsStream(fullName), Snag(s"Could not find resource $fullName"))
    iStreamShot.flatMap(is => Shot.fromTry(Try({
      val content = Source.fromInputStream(is)(Codec.UTF8).mkString
      LoadResponseMessage(name, content, s"Got '$name' from package '$packageName'.")
    })))
  }

  def getUrl(name: String): Shot[URL] = {
    val fullName = nameToFullName(name)
    Shot.notNull(classLoader.getResource(fullName), Snag(s"Could not get URL for $fullName"))
  }

  def getSomeUrl: Shot[URL] = Shots.findHit(entries, getUrl)

  override def list: Seq[String] = entries
}
