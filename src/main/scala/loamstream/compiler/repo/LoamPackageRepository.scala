package loamstream.compiler.repo

import java.io.File
import java.net.URL
import java.nio.file.{Path, Paths}

import loamstream.loam.LoamScript
import loamstream.util.{Shot, Snag}

import scala.io.{Codec, Source}

/** A repository of Loam scripts based on a package
  *
  * @param packageName Name of package where Loam scripts are stored
  * @param entries     List of names of available Loam scripts
  * */
final case class LoamPackageRepository(packageName: String, entries: Seq[String]) extends LoamRepository {
  private val classLoader: ClassLoader = classOf[LoamPackageRepository].getClassLoader

  /** Human-readable description on where the scripts are stored */
  override def description: String = s"in package $packageName"

  override def list: Seq[String] = entries

  /** Loads Loam script of given name from repository  */
  override def load(name: String): Shot[LoamScript] = {
    val fullName = nameToFullName(name)
    val stream = classLoader.getResourceAsStream(fullName)

    val iStreamShot = Shot.notNull(stream, Snag(s"Could not find resource $fullName"))

    iStreamShot.map { is =>
      val content = Source.fromInputStream(is)(Codec.UTF8).mkString

      LoamScript(name, content)
    }
  }

  /** Converts name of Loam script into its resource name */
  private def nameToFullName(name: String): String = s"$packageName${File.separator}$name${LoamRepository.fileSuffix}"

  /** Tries to convert Loam script name into URL to its location */
  private def getUrl(name: String): Shot[URL] = {
    val fullName = nameToFullName(name)

    Shot.notNull(classLoader.getResource(fullName), Snag(s"Could not get URL for $fullName"))
  }

  /** Tries to obtain a URL to any available Loam script in this package */
  private def getSomeUrl: Shot[URL] = Shot.findHit(entries, getUrl)

  /** Tries to obtain the folder where class files are stored */
  private[repo] def shootForClassFolder: Shot[Path] = {
    def getEnclosingFolder(entry: String): Shot[Path] = {
      getUrl(entry).map { url =>
        Paths.get(url.toURI).getParent
      }
    }

    Shot.findHit(entries, getEnclosingFolder)
  }
}
