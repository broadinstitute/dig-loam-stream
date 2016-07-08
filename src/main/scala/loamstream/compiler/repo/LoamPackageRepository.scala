package loamstream.compiler.repo

import java.io.File
import java.net.URL
import java.nio.file.{Path, Paths}

import loamstream.compiler.messages.LoadResponseMessage
import loamstream.util.{Shot, Shots, Snag}

import scala.io.{Codec, Source}

/** A repository of Loam scripts based on a package
  *
  * @param packageName Name of package where Loam scripts are stored
  * @param entries     List of names of available Loam scripts
  **/
case class LoamPackageRepository(packageName: String, entries: Seq[String]) extends LoamRepository {
  val classLoader: ClassLoader = classOf[LoamPackageRepository].getClassLoader

  /** Converts name of Loam script into its resource name */
  def nameToFullName(name: String): String = s"$packageName${File.separator}$name${LoamRepository.fileSuffix}"

  override def load(name: String): Shot[LoadResponseMessage] = {
    val fullName = nameToFullName(name)
    val iStreamShot =
      Shot.notNull(classLoader.getResourceAsStream(fullName), Snag(s"Could not find resource $fullName"))
    iStreamShot.flatMap(is => Shot {
      val content = Source.fromInputStream(is)(Codec.UTF8).mkString
      LoadResponseMessage(name, content, s"Got '$name' from package '$packageName'.")
    })
  }

  /** Tries to convert Loam script name into URL to its location */
  def getUrl(name: String): Shot[URL] = {
    val fullName = nameToFullName(name)
    Shot.notNull(classLoader.getResource(fullName), Snag(s"Could not get URL for $fullName"))
  }

  /** Tries to obtain a URL to any available Loam script in this package */
  def getSomeUrl: Shot[URL] = Shots.findHit(entries, getUrl)

  /** Tries to obtain the folder where class files are stored */
  def shootForClassFolder: Shot[Path] =
    Shots.findHit[String, Path](entries, entry => getUrl(entry).flatMap(url => Shot {
      val entryPath = Paths.get(url.toURI)
      entryPath.getParent
    }))


  override def list: Seq[String] = entries
}
