package loamstream.util

import java.nio.file.{Path, Paths}

import scala.io.StdIn.readLine


/**
  * LoamStream
  * Created by oliverr on 2/24/2016.
  */
object FileAsker {

  def ask(fileDescription: String): Path = {
    println("Please enter path of " + fileDescription)
    Paths.get(readLine)
  }

}
