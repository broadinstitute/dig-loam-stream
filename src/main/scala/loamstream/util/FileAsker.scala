package loamstream.util

import java.nio.file.{Files, Path, Paths}

import utils.Loggable

import scala.io.StdIn.readLine


/**
  * LoamStream
  * Created by oliverr on 2/24/2016.
  */
object FileAsker extends Loggable {

  def askIfNotExist(path: Path, paths: Path*)(fileDescription: String): Path =
    askIfNotExist(path +: paths)(fileDescription)

  def askIfNotExist(paths: Seq[Path])(fileDescription: String): Path = {
    paths.find(Files.exists(_)) match {
      case Some(path) => path
      case None => ask(fileDescription)
    }
  }

  def askIfParentDoesNotExist(path: Path, paths: Path*)(fileDescription: String): Path =
    askIfParentDoesNotExist(path +: paths)(fileDescription)

  def askIfParentDoesNotExist(paths: Seq[Path])(fileDescription: String): Path = {
    paths.find(path => Some(path.getParent).exists(Files.exists(_))) match {
      case Some(pathFound) => pathFound
      case None => ask(fileDescription)
    }
  }

  def ask(fileDescription: String): Path = {
    debug("Please enter path of " + fileDescription)
    Paths.get(readLine())
  }

}
