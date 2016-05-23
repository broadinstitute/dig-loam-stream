package loamstream.util

import java.io.File
import java.net.URL

/**
  * LoamStream
  * Created by oliverr on 5/23/2016.
  */
object ClassPathFinder {

  def getClassPath(obj: AnyRef): String = getClassPath(getUrls(obj))

  def getClassPath(clazz: Class[_]): String = getClassPath(getUrls(clazz))

  def getClassPath(classLoader: ClassLoader): String = getClassPath(getUrls(classLoader))

  def getClassPath(urls: Seq[URL]) = if (urls.isEmpty) "." else urls.map(_.getFile).mkString(File.pathSeparator)

  def getUrls(obj: AnyRef): Seq[URL] = getUrls(obj.getClass)

  def getUrls(clazz: Class[_]): Seq[URL] = getUrls(clazz.getClassLoader)

  def getUrls(classLoader: ClassLoader): Seq[URL] = {
    val parent = classLoader.getParent
    val urlsAncestors = if (parent != null) getUrls(parent) else Seq.empty // scalastyle:ignore null
    val urlsThis = classLoader match {
      case urlClassLoader: java.net.URLClassLoader => urlClassLoader.getURLs.toSeq
      case _ => Seq.empty
    }
    urlsAncestors ++ urlsThis
  }

}
