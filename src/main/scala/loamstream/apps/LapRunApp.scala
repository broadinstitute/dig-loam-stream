package loamstream.apps

import java.nio.file.Paths

import scala.sys.process.stringToProcess

/**
  * LoamStream
  * Created by oliverr on 11/25/2015.
  */
object LapRunApp extends App {
  val cmdLine = """ls"""
  println("Current directory: " + Paths.get(".").toAbsolutePath().normalize().toString())
  println("Parent directory: " + Paths.get("..").toAbsolutePath().normalize().toString())
  val exitCode = cmdLine.!
  println("Exit code is " + exitCode)
}
